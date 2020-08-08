/**
 * MVEL 2.0
 * Copyright (C) 2007 The Codehaus
 * Mike Brock, Dhanji Prasanna, John Graham, Mark Proctor
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.java.plus.dag.core.base.mvel2.util;

import org.java.plus.dag.core.base.mvel2.ast.ASTNode;

import java.io.Serializable;

/**
 * The ASTIterator interface defines the functionality required by the enginer, for compiletime and runtime
 * operations.  Unlike other script implementations, MVEL does not use a completely normalized AST tree for
 * it's execution.  Instead, nodes are organized into a linear order and delivered via this iterator interface,
 * much like bytecode instructions.
 */
public interface ASTIterator extends Serializable {
    void reset();

    ASTNode nextNode();

    void skipNode();

    ASTNode peekNext();

    ASTNode peekNode();

    ASTNode peekLast();

    // public boolean peekNextTokenFlags(int flags);
    void back();

    ASTNode nodesBack(int offset);

    ASTNode nodesAhead(int offset);

    boolean hasMoreNodes();

    String showNodeChain();

    ASTNode firstNode();

    int size();

    int index();

    void finish();

    void addTokenNode(ASTNode node);

    void addTokenNode(ASTNode node1, ASTNode node2);

}
