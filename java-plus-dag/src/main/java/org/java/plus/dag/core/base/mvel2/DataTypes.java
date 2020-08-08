/**
 * MVEL 2.0
 * Copyright (C) 2007  MVFLEX/Valhalla Project and the Codehaus
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

package org.java.plus.dag.core.base.mvel2;

/**
 * Contains constants for standard internal types.
 */
public interface DataTypes {
    int NULL = -1;

    int OBJECT = 0;
    int STRING = 1;
    int SHORT = 100;
    int INTEGER = 101;
    int LONG = 102;
    int DOUBLE = 103;
    int FLOAT = 104;
    int BOOLEAN = 7;
    int CHAR = 8;
    int BYTE = 9;

    int W_BOOLEAN = 15;

    int COLLECTION = 50;

    int W_SHORT = 105;
    int W_INTEGER = 106;
    int W_LONG = 107;
    int W_FLOAT = 108;
    int W_DOUBLE = 109;

    int W_CHAR = 112;
    int W_BYTE = 113;

    int BIG_DECIMAL = 110;
    int BIG_INTEGER = 111;

    int EMPTY = 200;

    int UNIT = 300;
}
