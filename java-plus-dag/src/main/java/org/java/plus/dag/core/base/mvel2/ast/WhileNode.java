/**
 * MVEL 2.0
 * Copyright (C) 2007 The Codehaus
 * Mike Brock, Dhanji Prasanna, John Graham, Mark Proctor
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.java.plus.dag.core.base.mvel2.ast;

import org.java.plus.dag.core.base.mvel2.ParserContext;
import org.java.plus.dag.core.base.mvel2.compiler.ExecutableStatement;
import org.java.plus.dag.core.base.mvel2.integration.VariableResolverFactory;
import org.java.plus.dag.core.base.mvel2.integration.impl.MapVariableResolverFactory;

import java.util.HashMap;

import static org.java.plus.dag.core.base.mvel2.util.CompilerTools.expectType;
import static org.java.plus.dag.core.base.mvel2.util.ParseTools.subCompileExpression;

/**
 * @author Christopher Brock
 */
public class WhileNode extends BlockNode {
    protected String item;
    protected ExecutableStatement condition;

    public WhileNode(char[] expr, int start, int offset, int blockStart, int blockEnd, int fields, ParserContext pCtx) {
        super(pCtx);
        expectType(pCtx, this.condition = (ExecutableStatement)subCompileExpression(expr, start, offset, pCtx),
            Boolean.class, ((fields & COMPILE_IMMEDIATE) != 0));

        if (pCtx != null) {
            pCtx.pushVariableScope();
        }
        this.compiledBlock = (ExecutableStatement)subCompileExpression(expr, blockStart, blockEnd, pCtx);

        if (pCtx != null) {
            pCtx.popVariableScope();

        }
    }

    public Object getReducedValueAccelerated(Object ctx, Object thisValue, VariableResolverFactory factory) {
        VariableResolverFactory ctxFactory = new MapVariableResolverFactory(new HashMap<String, Object>(), factory);
        while ((Boolean)condition.getValue(ctx, thisValue, factory)) {
            compiledBlock.getValue(ctx, thisValue, ctxFactory);
        }

        return null;
    }

    public Object getReducedValue(Object ctx, Object thisValue, VariableResolverFactory factory) {
        VariableResolverFactory ctxFactory = new MapVariableResolverFactory(new HashMap<String, Object>(), factory);

        while ((Boolean)condition.getValue(ctx, thisValue, factory)) {
            compiledBlock.getValue(ctx, thisValue, ctxFactory);
        }
        return null;
    }

}