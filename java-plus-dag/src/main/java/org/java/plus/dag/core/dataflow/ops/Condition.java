package org.java.plus.dag.core.dataflow.ops;

import java.util.function.Function;
import java.util.function.Supplier;

import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.dataflow.core.Operation;

/**
 * √Ë ˆ:
 * <p>
 * org.java.plus.dag.frame.dataflow.ops.Condition
 *
 * @author jaye
 * @date 2019/2/24
 * <p>
 * config_start: |org.java.plus.dag.frame.dataflow.ops.Condition||jaye| config_end:
 */
public class Condition<T> extends Operation<T> {
    private Operation<T> op;
    private Function<ProcessorContext, Boolean> condition;
    private Supplier<T> defaultVal;

    private Condition() {}

    private Condition(Operation<T> op, Function<ProcessorContext, Boolean> condition, Supplier<T> defaultVal) {
        this.op = op;
        this.condition = condition;
        this.defaultVal = defaultVal;
        depend(op);
    }

    @Override
    public T apply(ProcessorContext ctx) {
        if (condition.apply(ctx)) {
            return op.apply(ctx);
        } else {
            return defaultVal.get();
        }
    }
}