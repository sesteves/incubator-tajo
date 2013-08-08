/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner.physical;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.engine.eval.EvalContext;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

/**
 * This physical operator implements the hybrid hash join algorithm.
 */
public class HybridHashJoinExec extends BinaryPhysicalExec {

  private final static long WORKING_MEMORY = 1048576 * 128; // 128MB

  private JoinNode plan;
  private EvalNode joinQual;
  private FrameTuple frameTuple = new FrameTuple();
  private Tuple outTuple;

  private List<Column[]> joinKeyPairs;

  private EvalContext qualCtx;

  private Map<Tuple, List<Tuple>> tupleSlots;

  private int[] outerKeyList;
  private int[] innerKeyList;

  private Tuple outerTuple;
  private VTuple outerKeyTuple;

  List<ByteBuffer> innerBucketBuffers;
  List<ByteBuffer> outerBucketBuffers;
  private Iterator<Tuple> iterator;
  private boolean foundMatch = false;

  // projection
  private final Projector projector;
  private final EvalContext[] evalContexts;

  private int step = 1;

  private Map<Integer, List<Bucket>> bucketsMap = new TreeMap<Integer, List<Bucket>>();

  private Iterator<Integer> bucketsMapIterator;

  private boolean hasTuples = false;

  private boolean hasBuckets = false;

  private int bucketId = 0;

  private TableMeta innerTableMeta;
  private TableMeta outerTableMeta;

  public HybridHashJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer, PhysicalExec inner) {
    super(context, SchemaUtil.merge(outer.getSchema(), inner.getSchema()), plan.getOutSchema(), outer, inner);

    this.plan = plan;
    this.joinQual = plan.getJoinQual();
    this.qualCtx = joinQual.newContext();
    this.tupleSlots = new HashMap<Tuple, List<Tuple>>(10000);

    // contains the pairs of columns to join
    this.joinKeyPairs = PlannerUtil.getJoinKeyPairs(joinQual, outer.getSchema(), inner.getSchema());

    outerKeyList = new int[joinKeyPairs.size()];
    innerKeyList = new int[joinKeyPairs.size()];

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      outerKeyList[i] = outer.getSchema().getColumnId(joinKeyPairs.get(i)[0].getQualifiedName());
    }

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      innerKeyList[i] = inner.getSchema().getColumnId(joinKeyPairs.get(i)[1].getQualifiedName());
    }

    // for projection
    this.projector = new Projector(inSchema, outSchema, plan.getTargets());
    this.evalContexts = projector.renew();

    // for join
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.getColumnNum());
    outerKeyTuple = new VTuple(outerKeyList.length);

    outerKeyTuple = new VTuple(outerKeyList.length);

    innerTableMeta = CatalogUtil.newTableMeta(innerChild.outSchema, StoreType.CSV);
    outerTableMeta = CatalogUtil.newTableMeta(outerChild.outSchema, StoreType.CSV);

    // histogram partitioning
    Map<Integer, Long> histogram = context.getHistogram();

    int lastKey = -1;
    long accumulated = 0;
    List<Bucket> buckets;

    for (int key : histogram.keySet()) {
      long value = histogram.get(key);

      if (accumulated + value > WORKING_MEMORY) {

        if (accumulated > 0) {
          buckets = new ArrayList<Bucket>();
          buckets.add(new Bucket());
          bucketsMap.put(lastKey, buckets);
          accumulated = value;
        }

        if (value > WORKING_MEMORY) {
          // handle bucket overflow
          buckets = new ArrayList<Bucket>();

          Path outerPath = new Path(context.getWorkDir(), "outerBucket" + bucketId);

          long i = value / WORKING_MEMORY;
          while (i-- > 0) {
            buckets.add(new Bucket(outerPath));
          }
          bucketsMap.put(key, buckets);
          accumulated = 0;
        }
      } else {
        accumulated += value;
      }

      lastKey = key;
    }
    if (accumulated > 0) {
      buckets = new ArrayList<Bucket>();
      buckets.add(new Bucket());
      bucketsMap.put(lastKey, buckets);
    }

  }

  @Override
  public Tuple next() throws IOException {
    if (step++ == 1) {
      bucketInnerRelation();
    }

    if (step == 2) {
      Tuple outerTuple;
      if ((outerTuple = bucketOuterRelation()) == null) {
        step++;

        // close all appenders
        for (List<Bucket> buckets : bucketsMap.values()) {
          for (Bucket bucket : buckets) {
            bucket.getinnerAppender().close();
            bucket.getouterAppender().close();
          }
        }
      }
    }

    if (step == 3) {

      Scanner innerScanner, outerScanner;
      List<Bucket> buckets;
      Iterator<Bucket> bucketsIterator = null;

      if (bucketsMapIterator == null)
        bucketsMapIterator = bucketsMap.keySet().iterator();

      while (bucketsMapIterator.hasNext()) {

        if (!hasTuples && !hasBuckets) {
          buckets = bucketsMap.get(bucketsMapIterator.next());
          bucketsIterator = buckets.iterator();
        }

        while (bucketsIterator.hasNext()) {

          if (!hasTuples) {
            Bucket bucket = bucketsIterator.next();

            // load inner bucket
            innerScanner = bucket.getinnerScanner();
            tupleSlots.clear();

            outerScanner = bucket.getouterScanner();
            hasBuckets = bucketsIterator.hasNext();
          }

          // probe outer bucket
          while (true) {
            // check for match in tupleSlots

            // until match is found

            // hasTuples = .hasNext()
          }
        }
      }
      return null;
    }
    // return outerTuple;
    return null;
  }

  private void bucketInnerRelation() throws IOException {
    Tuple tuple;
    Tuple keyTuple;
    List<Bucket> buckets;
    Bucket bucket = null;

    while ((tuple = innerChild.next()) != null) {

      keyTuple = new VTuple(joinKeyPairs.size());
      for (int i = 0; i < innerKeyList.length; i++) {
        keyTuple.put(i, tuple.get(innerKeyList[i]));
      }

      buckets = getBuckets(keyTuple.hashCode());
      if (buckets.size() == 1) {
        bucket = buckets.get(0);
      } else {
        long tupleSize = getTupleSize(tuple);
        for (Bucket b : buckets) {
          long estimatedSize = b.getSize() + tupleSize;
          if (estimatedSize < WORKING_MEMORY) {
            b.setSize(estimatedSize);
            bucket = b;
            break;
          }
        }
      }

      if (bucket.isBucketZero()) {
        List<Tuple> tuples = tupleSlots.get(keyTuple);
        if (tuples == null) {
          tuples = new ArrayList<Tuple>();
        }
        tuples.add(tuple);
        tupleSlots.put(keyTuple, tuples);
      } else {
        // write tuple out to disk
        Appender appender = bucket.getinnerAppender();
        appender.addTuple(tuple);
      }
    }
  }

  private long getTupleSize(Tuple tuple) {
    long size = 0;
    for (int i = 0; i < tuple.size(); i++) {
      size += tuple.get(i).size();
    }
    return size;
  }

  private Tuple bucketOuterRelation() throws IOException {
    Tuple innerTuple;
    List<Tuple> tuples;
    List<Bucket> buckets;
    Bucket bucket;

    while (!foundMatch) {
      // getting new outer
      outerTuple = outerChild.next();
      if (outerTuple == null) {
        return null;
      }

      for (int i = 0; i < outerKeyList.length; i++) {
        outerKeyTuple.put(i, outerTuple.get(outerKeyList[i]));
      }
      buckets = getBuckets(outerKeyTuple.hashCode());
      bucket = buckets.get(0);

      if (bucket.isBucketZero()) {
        if (buckets.size() > 1) {
          // FIXME

        }
        // probe directly
        tuples = tupleSlots.get(outerKeyList);
        if (tuples != null) {
          iterator = tuples.iterator();
          break;
        }
      } else {
        // write tuple out to disk
        Appender appender = bucket.getouterAppender();
        appender.addTuple(outerTuple);

      }
    }

    innerTuple = iterator.next();
    frameTuple.set(outerTuple, innerTuple);
    joinQual.eval(qualCtx, inSchema, frameTuple);
    if (joinQual.terminate(qualCtx).asBool()) {
      projector.eval(evalContexts, frameTuple);
      projector.terminate(evalContexts, outTuple);
    }
    foundMatch = iterator.hasNext();

    return outTuple;
  }

  private List<Bucket> getBuckets(int hashCode) {
    for (int key : bucketsMap.keySet()) {
      if (hashCode < key) {
        return bucketsMap.get(key);
      }
    }
    return null;
  }

  private class Bucket {

    // private ByteBuffer innerRelationBuffer;
    // private ByteBuffer outerRelationBuffer;

    private Appender innerAppender;

    private Appender outerAppender;

    private Scanner innerScanner;

    private Scanner outerScanner;

    private long size = 0;

    private boolean bucketZero = false;

    public Bucket() {
      this(new Path(context.getWorkDir(), "outerBucket" + bucketId));
    }

    public Bucket(Path outerPath) {
      try {
        Path path = new Path(context.getWorkDir(), "innerBucket" + bucketId++);
        this.innerAppender = StorageManager.getAppender(context.getConf(), innerTableMeta, path);
        this.innerAppender.init();
        this.innerScanner = StorageManager.getScanner(context.getConf(), innerTableMeta, path);
        this.innerScanner.init();

        this.outerAppender = StorageManager.getAppender(context.getConf(), innerTableMeta, outerPath);
        this.outerAppender.init();
        this.outerScanner = StorageManager.getScanner(context.getConf(), outerTableMeta, outerPath);
        this.outerScanner.init();

      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public long getSize() {
      return size;
    }

    public void setSize(long size) {
      this.size = size;
    }

    public boolean isBucketZero() {
      return bucketZero;
    }

    public void setBucketZero(boolean bucketZero) {
      this.bucketZero = bucketZero;
    }

    public Appender getinnerAppender() {
      return innerAppender;
    }

    public Appender getouterAppender() {
      return outerAppender;
    }

    public Scanner getinnerScanner() {
      return innerScanner;
    }

    public Scanner getouterScanner() {
      return outerScanner;
    }

  }
}
