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
import org.apache.tajo.json.GsonObject;

public abstract class BinaryNode extends LogicalNode implements Cloneable, GsonObject {
	@Expose LogicalNode leftChild = null;
	@Expose LogicalNode inner = null;
	
	public BinaryNode() {
		super();
	}
	
	/**
	 * @param opType
	 */
	public BinaryNode(NodeType opType) {
		super(opType);
	}
	
	public LogicalNode getLeftChild() {
		return this.leftChild;
	}
	
	public void setLeftChild(LogicalNode op) {
		this.leftChild = op;
	}

	public LogicalNode getRightChild() {
		return this.inner;
	}

	public void setRightChild(LogicalNode op) {
		this.inner = op;
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
	  BinaryNode binNode = (BinaryNode) super.clone();
	  binNode.leftChild = (LogicalNode) leftChild.clone();
	  binNode.inner = (LogicalNode) inner.clone();
	  
	  return binNode;
	}
	
	public void preOrder(LogicalNodeVisitor visitor) {
	  visitor.visit(this);
	  leftChild.postOrder(visitor);
    inner.postOrder(visitor);    
  }
	
	public void postOrder(LogicalNodeVisitor visitor) {
    leftChild.postOrder(visitor);
    inner.postOrder(visitor);
    visitor.visit(this);
  }
}
