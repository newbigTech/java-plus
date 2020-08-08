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

package org.java.plus.dag.core.base.mvel2.optimizers;

import org.java.plus.dag.core.base.mvel2.ParserContext;
import org.java.plus.dag.core.base.mvel2.compiler.Accessor;
import org.java.plus.dag.core.base.mvel2.integration.VariableResolverFactory;

public interface AccessorOptimizer {
    void init();

    Accessor optimizeAccessor(ParserContext pCtx, char[] property, int start, int offset, Object ctx, Object thisRef,
                              VariableResolverFactory factory, boolean rootThisRef, Class ingressType);

    Accessor optimizeSetAccessor(ParserContext pCtx, char[] property, int start, int offset, Object ctx, Object thisRef,
                                 VariableResolverFactory factory, boolean rootThisRef, Object value, Class ingressType);

    Accessor optimizeCollection(ParserContext pCtx, Object collectionGraph, Class type, char[] property, int start,
                                int offset, Object ctx, Object thisRef, VariableResolverFactory factory);

    Accessor optimizeObjectCreation(ParserContext pCtx, char[] property, int start, int offset, Object ctx,
                                    Object thisRef, VariableResolverFactory factory);

    Object getResultOptPass();

    Class getEgressType();

    boolean isLiteralOnly();
}
