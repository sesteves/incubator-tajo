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

import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.engine.eval.EvalContext;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

/**
 * This physical operator implements the hybrid hash join algorithm.
 */
public class HybridHashJoinExec extends BinaryPhysicalExec {

  private final static long WORKING_MEMORY = 1048576 * 128; // 128MB

  private JoinNode plan;
  private EvalNode joinQual;

  private List<Column[]> joinKeyPairs;

  private EvalContext qualCtx;
  private boolean first = true;

  private Map<Tuple, List<Tuple>> tupleSlots;

  private int[] outerKeyList;
  private int[] innerKeyList;

  private VTuple outerKeyTuple;

  List<ByteBuffer> innerBucketBuffers;
  List<ByteBuffer> outerBucketBuffers;
  private int currentBucket;
  private Iterator<Tuple> iterator;
  private boolean foundMatch = false;
  private int step = 1;

  private Map<Integer, List<Integer>> buckets = new TreeMap<Integer, List<Integer>>();

  private Map<Integer, Long> bucketByteCounter = new HashMap<Integer, Long>();

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

    outerKeyTuple = new VTuple(outerKeyList.length);

    innerBucketBuffers = new ArrayList<ByteBuffer>();
    outerBucketBuffers = new ArrayList<ByteBuffer>();

    // histogram partitioning
    Map<Integer, Long> histogram = context.getHistogram();

    int bucketId = 0;
    int lastKey = -1;
    long accumulated = 0;
    List<Integer> bucketIds;

    for (int key : histogram.keySet()) {
      long value = histogram.get(key);

      if (accumulated + value > WORKING_MEMORY) {

        if (accumulated > 0) {
          bucketIds = new ArrayList<Integer>();
          bucketIds.add(bucketId++);
          buckets.put(lastKey, bucketIds);
          accumulated = value;
        }

        if (value > WORKING_MEMORY) {
          // handle bucket overflow
          bucketIds = new ArrayList<Integer>();
          long i = value / WORKING_MEMORY;
          while (i-- > 0) {
            bucketIds.add(bucketId);
            bucketByteCounter.put(bucketId++, 0L);
          }
          buckets.put(key, bucketIds);

          accumulated = 0;
        }
      } else {
        accumulated += value;
      }

      lastKey = key;
    }

  }

  @Override
  public Tuple next() throws IOException {
    if (step++ == 1) {
      bucketInnerRelation();
    }

    Tuple outerTuple;
    if (step == 2) {
      if ((outerTuple = bucketOuterRelation()) == null) {
        step++;
      }
    }

    if (step == 3) {

      List<Tuple> tupleList;

      // for each tuple in currentPartition
      // load all tuples into hashmap

      // currentPartition

      // load inner bucket
      // if (tupleSlots.containsKey(key)) {
      // tupleList = tupleSlots.get(key);
      // tupleList.add(null);
      // } else {
      // tupleList = new ArrayList<Tuple>();
      // tupleSlots.put(key, tupleList);
      // }

      // probe outer bucket
      for (ByteBuffer buffer : outerBucketBuffers) {

      }

    }
    // return outerTuple;
    return null;
  }

  private void bucketInnerRelation() throws IOException {
    Tuple tuple;
    Tuple keyTuple;
    List<Integer> bucketIds;
    int bucketId = -1;

    while ((tuple = innerChild.next()) != null) {

      keyTuple = new VTuple(joinKeyPairs.size());
      List<Tuple> newValue;
      for (int i = 0; i < innerKeyList.length; i++) {
        keyTuple.put(i, tuple.get(innerKeyList[i]));
      }

      bucketIds = getBucketId(keyTuple.hashCode());
      if (bucketIds.size() == 1) {
        bucketId = bucketIds.get(0);
      } else {
        long tupleSize = getTupleSize(tuple);
        for (int bId : bucketIds) {
          long estimatedSize = bucketByteCounter.get(bId) + tupleSize;
          if (estimatedSize < WORKING_MEMORY) {
            bucketByteCounter.put(bId, estimatedSize);
            bucketId = bId;
            break;
          }
        }
      }

      if (bucketId == 0) {
        if (tupleSlots.containsKey(keyTuple)) {
          newValue = tupleSlots.get(keyTuple);
          newValue.add(tuple);
          // tupleSlots.put(keyTuple, newValue);
        } else {
          newValue = new ArrayList<Tuple>();
          newValue.add(tuple);
          tupleSlots.put(keyTuple, newValue);
        }
      } else {
        ByteBuffer buffer = innerBucketBuffers.get(bucketId);
        // write tuple out to disk

      }

    }
    first = false;
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
    Tuple outerTuple;
    int bucketId = -1;

    while (!foundMatch) {
      // getting new outer
      outerTuple = outerChild.next();
      if (outerTuple == null) {
        return null;
      }

      for (int i = 0; i < outerKeyList.length; i++) {
        outerKeyTuple.put(i, outerTuple.get(outerKeyList[i]));
      }
      // bucketId = getBucketId(outerKeyTuple.hashCode());

      // == partition zero
      if (bucketId == currentBucket) { // probe directly
        if (tupleSlots.containsKey(outerKeyTuple)) {
          iterator = tupleSlots.get(outerKeyList).iterator();
          break;
        }
      } else {
        ByteBuffer buffer = outerBucketBuffers.get(bucketId);
        // write tuple out to disk

      }
    }

    // innerTuple = iterator.next();
    // frameTuple.set(outerTuple, innerTuple);
    // joinQual.eval(qualCtx, inSchema, frameTuple);
    // if (joinQual.terminate(qualCtx).asBool()) {
    // projector.eval(evalContexts, frameTuple);
    // projector.terminate(evalContexts, outTuple);
    // }
    foundMatch = iterator.hasNext();

    // return outerTuple;
    return null;
  }

  private List<Integer> getBucketId(int hashCode) {
    for (int key : buckets.keySet()) {
      if (hashCode < key) {
        return buckets.get(key);
      }
    }
    return null;
  }
}
