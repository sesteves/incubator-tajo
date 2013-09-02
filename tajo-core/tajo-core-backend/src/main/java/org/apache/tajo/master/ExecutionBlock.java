/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.master;

import com.google.common.base.Preconditions;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.planner.logical.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A distributed execution plan (DEP) is a direct acyclic graph (DAG) of ExecutionBlocks.
 * An ExecutionBlock is a basic execution unit that could be distributed across a number of nodes.
 * An ExecutionBlock class contains input information (e.g., child execution blocks or input
 * tables), and output information (e.g., partition type, partition key, and partition number).
 * In addition, it includes a logical plan to be executed in each node.
 */
public class ExecutionBlock {

  public static enum PartitionType {
	/** for hash partitioning */
	HASH, LIST,
	/** for map-side join */
	BROADCAST,
	/** for range partitioning */
	RANGE
  }

  private ExecutionBlockId executionBlockId;
  private LogicalNode plan = null;
  private StoreTableNode store = null;
  private List<ScanNode> scanlist = new ArrayList<ScanNode>();
  private ExecutionBlock parent;
  private Map<ScanNode, ExecutionBlock> childSubQueries = new HashMap<ScanNode, ExecutionBlock>();
  private PartitionType outputType;
  private boolean hasJoinPlan;
  private boolean hasUnionPlan;
  private List<Column[]> joinKeyPairs = null;
  // histogram that is maintained for the smaller relation of a join
  private Map<Integer, Long> histogram = null;

  public ExecutionBlock(ExecutionBlockId executionBlockId) {
    this.executionBlockId = executionBlockId;
  }

  public ExecutionBlockId getId() {
    return executionBlockId;
  }

  public String getOutputName() {
    return store.getTableName();
  }

  public void setPartitionType(PartitionType partitionType) {
    this.outputType = partitionType;
  }

  public PartitionType getPartitionType() {
    return outputType;
  }

  public void setPlan(LogicalNode plan) {

    hasJoinPlan = false;
    Preconditions.checkArgument(plan.getType() == NodeType.STORE);

    this.plan = plan;
    store = (StoreTableNode) plan;

    LogicalNode node = plan;
    ArrayList<LogicalNode> s = new ArrayList<LogicalNode>();
    s.add(node);
    while (!s.isEmpty()) {
      node = s.remove(s.size()-1);
      if (node instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) node;
        s.add(s.size(), unary.getChild());
      } else if (node instanceof BinaryNode) {
        BinaryNode binary = (BinaryNode) node;
        if (binary.getType() == NodeType.JOIN) {
          hasJoinPlan = true;
		  JoinNode joinNode = (JoinNode) node;
		  joinKeyPairs = PlannerUtil.getJoinKeyPairs(joinNode.getJoinQual(), joinNode.getOuterNode().getOutSchema(),
			  joinNode.getInnerNode().getOutSchema());
        } else if (binary.getType() == NodeType.UNION) {
          hasUnionPlan = true;
        }
        s.add(s.size(), binary.getLeftChild());
        s.add(s.size(), binary.getRightChild());
      } else if (node instanceof ScanNode) {
        scanlist.add((ScanNode)node);
      }
    }
  }

  public LogicalNode getPlan() {
    return plan;
  }

  public boolean hasParentBlock() {
    return parent != null;
  }

  public ExecutionBlock getParentBlock() {
    return parent;
  }

  public void setParentBlock(ExecutionBlock parent) {
    this.parent = parent;
  }

  public boolean hasChildBlock() {
    return childSubQueries.size() > 0;
  }

  public ExecutionBlock getChildBlock(ScanNode scanNode) {
    return childSubQueries.get(scanNode);
  }

  public Collection<ExecutionBlock> getChildBlocks() {
    return Collections.unmodifiableCollection(childSubQueries.values());
  }

  public Map<ScanNode, ExecutionBlock> getChildBlockMap() {
    return childSubQueries;
  }

  public void addChildBlock(ScanNode scanNode, ExecutionBlock child) {
    childSubQueries.put(scanNode, child);
  }

  public int getChildNum() {
    return childSubQueries.size();
  }

  public void removeChildBlock(ScanNode scanNode) {
    scanlist.remove(scanNode);
    this.childSubQueries.remove(scanNode);
  }

  public void addChildBlocks(Map<ScanNode, ExecutionBlock> childBlocks) {
    childSubQueries.putAll(childBlocks);
  }

  public boolean isLeafBlock() {
    return childSubQueries.size() == 0;
  }

  public StoreTableNode getStoreTableNode() {
    return store;
  }

  public ScanNode [] getScanNodes() {
    return this.scanlist.toArray(new ScanNode[scanlist.size()]);
  }

  public Schema getOutputSchema() {
    return store.getOutSchema();
  }

  public boolean hasJoin() {
    return hasJoinPlan;
  }

  public boolean hasUnion() {
	return hasUnionPlan;
  }

  public List<Column[]> getJoinKeyPairs() {
	return joinKeyPairs;
  }

  public Map<Integer, Long> getHistogram() {
	return histogram;
  }

  public void setHistogram(Map<Integer, Long> histogram) {
	this.histogram = histogram;
  }
}
