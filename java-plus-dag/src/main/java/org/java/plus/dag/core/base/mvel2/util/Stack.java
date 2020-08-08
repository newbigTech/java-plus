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

import java.io.Serializable;

public interface Stack extends Serializable {
    boolean isEmpty();

    Object peek();

    Object peek2();

    void add(Object obj);

    void push(Object obj);

    Object pushAndPeek(Object obj);

    void push(Object obj1, Object obj2);

    void push(Object obj1, Object obj2, Object obj3);

    Object pop();

    Object pop2();

    void discard();

    void clear();

    int size();

    void showStack();
}
