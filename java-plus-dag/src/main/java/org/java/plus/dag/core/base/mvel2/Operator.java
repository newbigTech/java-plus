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
package org.java.plus.dag.core.base.mvel2;

/**
 * Contains a list of constants representing internal operators.
 */
public interface Operator {

    int NOOP = -1;

    /**
     * The index positions of the operator precedence values
     * correspond to the actual operator itself. So ADD is PTABLE[0],
     * SUB is PTABLE[1] and so on.
     */
    int[] PTABLE = {
        10,   // ADD
        10,   // SUB
        11,   // MULT
        11,   // DIV
        11,   // MOD
        12,   // POWER

        6,   // BW_AND
        4,   // BW_OR
        5,   // BW_XOR
        9,   // BW_SHIFT_RIGHT
        9,   // BW_SHIFT_LEFT
        9,   // BW_USHIFT_RIGHT
        9,   // BW_USHIFT_LEFT
        5,   // BW_NOT

        8,   // LTHAN
        8,   // GTHAN
        8,   // LETHAN
        8,   // GETHAN

        7,   // EQUAL
        7,   // NEQUAL

        13,    // STR_APPEND
        3,   // AND
        2,   // OR
        2,   // CHOR
        13,   // REGEX
        8,   // INSTANCEOF
        13,   // CONTAINS
        13,   // SOUNDEX
        13,   // SIMILARITY

        0,  // TERNARY
        0,  // TERNARY ELSE
        13,   // ASSIGN
        13,   // INC_ASSIGN
        13   // DEC ASSIGN

    };

    int ADD = 0;
    int SUB = 1;
    int MULT = 2;
    int DIV = 3;
    int MOD = 4;
    int POWER = 5;

    int BW_AND = 6;
    int BW_OR = 7;
    int BW_XOR = 8;
    int BW_SHIFT_RIGHT = 9;
    int BW_SHIFT_LEFT = 10;
    int BW_USHIFT_RIGHT = 11;
    int BW_USHIFT_LEFT = 12;
    int BW_NOT = 13;

    int LTHAN = 14;
    int GTHAN = 15;
    int LETHAN = 16;
    int GETHAN = 17;

    int EQUAL = 18;
    int NEQUAL = 19;

    int STR_APPEND = 20;
    int AND = 21;
    int OR = 22;
    int CHOR = 23;
    int REGEX = 24;
    int INSTANCEOF = 25;
    int CONTAINS = 26;
    int SOUNDEX = 27;
    int SIMILARITY = 28;

    int TERNARY = 29;
    int TERNARY_ELSE = 30;
    int ASSIGN = 31;
    int INC_ASSIGN = 32;
    int DEC_ASSIGN = 33;
    int NEW = 34;
    int PROJECTION = 35;
    int CONVERTABLE_TO = 36;
    int END_OF_STMT = 37;

    int FOREACH = 38;
    int IF = 39;
    int ELSE = 40;
    int WHILE = 41;
    int UNTIL = 42;
    int FOR = 43;
    int SWITCH = 44;
    int DO = 45;
    int WITH = 46;
    int ISDEF = 47;

    int PROTO = 48;

    int INC = 50;
    int DEC = 51;
    int ASSIGN_ADD = 52;
    int ASSIGN_SUB = 53;
    int ASSIGN_STR_APPEND = 54;
    int ASSIGN_DIV = 55;
    int ASSIGN_MOD = 56;

    int ASSIGN_OR = 57;
    int ASSIGN_AND = 58;
    int ASSIGN_XOR = 59;
    int ASSIGN_LSHIFT = 60;
    int ASSIGN_RSHIFT = 61;
    int ASSIGN_RUSHIFT = 62;

    int IMPORT_STATIC = 95;
    int IMPORT = 96;
    int ASSERT = 97;
    int UNTYPED_VAR = 98;
    int RETURN = 99;

    int FUNCTION = 100;
    int STACKLANG = 101;
    int PUSH = 102;
    int POP = 103;
    int LOAD = 104;
    int LDTYPE = 105;
    int INVOKE = 106;
    int GETFIELD = 107;
    int STOREFIELD = 108;
    int STORE = 109;
    int DUP = 110;
    int LABEL = 111;
    int JUMP = 112;
    int JUMPIF = 113;
    int REDUCE = 114;
    int SWAP = 115;
    int XSWAP = 116;

}
