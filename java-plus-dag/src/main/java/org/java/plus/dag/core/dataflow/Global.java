package org.java.plus.dag.core.dataflow;

import java.util.Objects;

import org.java.plus.dag.core.dataflow.core.Operation;

/**
 * √Ë ˆ:
 * <p>
 * org.java.plus.dag.frame.dataflow.Global
 *
 * @author jaye
 * @date 2019/2/20
 * <p>
 * config_start: |org.java.plus.dag.frame.dataflow.Global||jaye| config_end:
 */
public class Global {
    public static void requireOpNonNull(Operation... ops) {
        for (Operation op : ops) {
            Objects.requireNonNull(ops);
            Objects.requireNonNull(op.get());
        }
    }

    public static boolean isOpsNull(Operation... ops) {
        for (Operation op : ops) {
            if (null == op) { return true; }
            if (null == op.get()) { return true; }
        }
        return false;
    }

    public static void requireNonNull(Object... objs) {
        for (Object obj : objs) { Objects.requireNonNull(obj); }
    }

    public static boolean isNull(Object... objs) {
        for (Object obj : objs) {
            if (null == obj) { return true; }
        }
        return false;
    }
}