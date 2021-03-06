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

/**
 * 
 */
package org.apache.tajo.engine.planner.logical;

import com.google.gson.annotations.Expose;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.Target;

public class JoinNode extends BinaryNode implements Projectable, Cloneable {
  @Expose private JoinType joinType;
  @Expose private EvalNode joinQual;
  @Expose private Target[] targets;

  public JoinNode(JoinType joinType, LogicalNode left) {
    super(NodeType.JOIN);
    this.joinType = joinType;
    setLeftChild(left);
  }

  public JoinNode(JoinType joinType, LogicalNode left, LogicalNode right) {
    super(NodeType.JOIN);
    this.joinType = joinType;
    setLeftChild(left);
    setRightChild(right);
  }

  public JoinType getJoinType() {
    return this.joinType;
  }

  public void setJoinType(JoinType joinType) {
    this.joinType = joinType;
  }

  public void setJoinQual(EvalNode joinQual) {
    this.joinQual = joinQual;
  }

  public boolean hasJoinQual() {
    return this.joinQual != null;
  }

  public EvalNode getJoinQual() {
    return this.joinQual;
  }

  @Override
  public boolean hasTargets() {
    return this.targets != null;
  }

  @Override
  public Target[] getTargets() {
    return this.targets;
  }

  @Override
  public void setTargets(Target[] targets) {
    this.targets = targets;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof JoinNode) {
      JoinNode other = (JoinNode) obj;
      return super.equals(other) && leftChild.equals(other.leftChild)
          && inner.equals(other.inner);
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    JoinNode join = (JoinNode) super.clone();
    join.joinType = this.joinType;
    join.joinQual = this.joinQual == null ? null : (EvalNode) this.joinQual.clone();
    return join;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\"Join\": \"joinType\": \" ").append(joinType).append("\"");
    if (joinQual != null) {
      sb.append(", \"qual\": ").append(joinQual);
    }
    if (targets != null) {
      sb.append(", \"target list\": ");
      boolean first = true;
      for (Target target : targets) {
        if (!first) {
          sb.append(", ");
        }
        sb.append(target);
        first = false;
      }
    }

    sb.append("\n\"out schema: ").append(getOutSchema());
    sb.append("\n\"in schema: ").append(getInSchema());
    sb.append("\n" + getLeftChild().toString()).append(" and ").append(getRightChild());
    return sb.toString();
  }
}
