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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.engine.eval.EvalContext;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

public class HashJoinExec extends BinaryPhysicalExec {
  // from logical plan
  private JoinNode plan;
  private EvalNode joinQual;

  private List<Column[]> joinKeyPairs;

  // temporal tuples and states for nested loop join
  private boolean first = true;
  private FrameTuple frameTuple;
  private Tuple outTuple = null;
  private Map<Tuple, List<Tuple>> tupleSlots;
  private Iterator<Tuple> iterator = null;
  private EvalContext qualCtx;
  private Tuple outerTuple;
  private Tuple outerKeyTuple;

  private int[] outerKeyList;
  private int[] innerKeyList;

  private boolean finished = false;
  boolean nextOuter = true;

  // projection
  private final Projector projector;
  private final EvalContext[] evalContexts;

  public HashJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer, PhysicalExec inner) {
	super(context, SchemaUtil.merge(outer.getSchema(), inner.getSchema()), plan.getOutSchema(), outer, inner);
	this.plan = plan;
	this.joinQual = plan.getJoinQual();
	this.qualCtx = joinQual.newContext();
	this.tupleSlots = new HashMap<Tuple, List<Tuple>>(10000);

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

	Map<Integer, Long> histogram = context.getHistogram();
	if (histogram != null) {
	  for (int key : histogram.keySet())
		System.out.println("### histogram: " + key + " :: " + histogram.get(key));
	}
  }

  private void getKeyOuterTuple(final Tuple outerTuple, Tuple keyTuple) {
	for (int i = 0; i < outerKeyList.length; i++) {
	  keyTuple.put(i, outerTuple.get(outerKeyList[i]));
	}
  }

  public Tuple next() throws IOException {
	if (first) {
	  loadInnerTable();
	}

	Tuple innerTuple;
	boolean found = false;

	while (!finished) {

	  if (nextOuter) {
		// getting new outer
		outerTuple = outerChild.next();
		if (outerTuple == null) {
		  finished = true;
		  return null;
		}

		// getting corresponding inner
		getKeyOuterTuple(outerTuple, outerKeyTuple);
		if (tupleSlots.containsKey(outerKeyTuple)) {
		  iterator = tupleSlots.get(outerKeyTuple).iterator();
		  nextOuter = false;
		} else {
		  nextOuter = true;
		  continue;
		}
	  }

	  // getting next inner tuple
	  innerTuple = iterator.next();
	  frameTuple.set(outerTuple, innerTuple);
	  joinQual.eval(qualCtx, inSchema, frameTuple);
	  if (joinQual.terminate(qualCtx).asBool()) {
		projector.eval(evalContexts, frameTuple);
		projector.terminate(evalContexts, outTuple);
		found = true;
	  }

	  if (!iterator.hasNext()) { // no more inner tuple
		nextOuter = true;
	  }

	  if (found) {
		break;
	  }
	}

	return outTuple;
  }

  private void loadInnerTable() throws IOException {
	Tuple tuple;
	Tuple keyTuple;

	while ((tuple = innerChild.next()) != null) {
	  keyTuple = new VTuple(joinKeyPairs.size());
	  List<Tuple> newValue;
	  for (int i = 0; i < innerKeyList.length; i++) {
		keyTuple.put(i, tuple.get(innerKeyList[i]));
	  }

	  if (tupleSlots.containsKey(keyTuple)) {
		newValue = tupleSlots.get(keyTuple);
		newValue.add(tuple);
		tupleSlots.put(keyTuple, newValue);
	  } else {
		newValue = new ArrayList<Tuple>();
		newValue.add(tuple);
		tupleSlots.put(keyTuple, newValue);
	  }
	}
	first = false;
  }

  @Override
  public void rescan() throws IOException {
	super.rescan();

	tupleSlots.clear();
	first = true;

	finished = false;
	iterator = null;
	nextOuter = true;
  }

  public void close() throws IOException {
	tupleSlots.clear();
  }

  public JoinNode getPlan() {
	return this.plan;
  }
}
