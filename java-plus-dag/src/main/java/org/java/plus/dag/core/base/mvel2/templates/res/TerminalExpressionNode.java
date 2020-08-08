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

package org.java.plus.dag.core.base.mvel2.templates.res;

import org.java.plus.dag.core.base.mvel2.MVEL;
import org.java.plus.dag.core.base.mvel2.integration.VariableResolverFactory;
import org.java.plus.dag.core.base.mvel2.templates.TemplateRuntime;
import org.java.plus.dag.core.base.mvel2.templates.util.TemplateOutputStream;

public class TerminalExpressionNode extends Node {
    public TerminalExpressionNode() {
    }

    public TerminalExpressionNode(Node node) {
        this.begin = node.begin;
        this.name = node.name;
        this.contents = node.contents;
        this.cStart = node.cStart;
        this.cEnd = node.cEnd;
    }

    public Object eval(TemplateRuntime runtime, TemplateOutputStream appender, Object ctx,
                       VariableResolverFactory factory) {
        return MVEL.eval(contents, cStart, cEnd - cStart, ctx, factory);
    }

    public boolean demarcate(Node terminatingNode, char[] template) {
        return false;
    }
}
