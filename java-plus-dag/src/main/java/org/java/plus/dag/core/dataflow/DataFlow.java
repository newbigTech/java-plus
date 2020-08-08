package org.java.plus.dag.core.dataflow;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.dataflow.core.Operation;
import org.java.plus.dag.core.dataflow.ops.InnerJoin;
import org.java.plus.dag.core.dataflow.ops.ReadDataSource;
import org.java.plus.dag.core.dataflow.ops.RunProcessor;
import org.java.plus.dag.core.dataflow.ops.SplitShow;
import org.java.plus.dag.core.dataflow.ops.SplitVdo;

/**
 * √Ë ˆ: DataSet Flow org.java.plus.dag.frame.dataflow.DataFlow
 *
 * @author jaye
 * @date 2019/2/20
 * <p>
 * config_start: |org.java.plus.dag.frame.dataflow.DataFlow|DataSet Flow|jaye| config_end:
 */
public abstract class DataFlow {
    public abstract <T> T run(Operation<T> op);
    public abstract <T> T run(Operation<T> op, Supplier<T> defaultResult);
    public abstract <T> T eval(Operation<T> op);
    public abstract <T> T eval(Operation<T> op, Supplier<T> defaultVal);
    public abstract void reset();


    // ----------util functions--------- //


    /**
     * create an op depended on the other ops
     * @param func the op function the will execute in the new op
     * @param firstOp the first depended op, this param just to remind the user not to forget to add dependency
     * @param ops the other depended ops
     * @return new op
     */
    public static <T> Operation<T> newOp(Function<ProcessorContext, T> func, Function<Throwable, T> errHandler,
                                         Operation firstOp, Operation... ops) {
        Objects.requireNonNull(func);
        Objects.requireNonNull(errHandler);
        return new Operation<T>() {
            @Override
            public T apply(ProcessorContext ctx) {
                return func.apply(ctx);
            }
        }.depend(firstOp).depend(ops).handleErr(errHandler).name(getStackTraceName());
    }

    public static <T> Operation<T> newOpDependNone(Function<ProcessorContext, T> func,
                                                   Function<Throwable, T> errHandler) {
        return newOp(func, errHandler, null).name(getStackTraceName());
    }

    private static String getStackTraceName() {
        StringBuilder name = new StringBuilder();
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if (element.getClassName().equals(DataFlow.class.getName())) { continue; }
            if (element.getClassName().equals(Thread.class.getName())) { continue; }
            name.append(element.getClassName()).append(":").append(element.getMethodName()).append(":")
                .append(element.getLineNumber());
            break;
        }
        // return null is ok because name() method will ignore null
        if (name.length() <= 0) { return null; }
        return name.toString();
    }

    public static <T> Operation<T> input(T input) {
        return new Operation<T>() {
            @Override
            public T apply(ProcessorContext ctx) {
                return input;
            }
        }.name("DataFlow.input");
    }

    public Operation<DataSet<Row>> sort(Operation<DataSet<Row>> input) {
        return DataFlow.newOp(ctx -> {
            Global.requireOpNonNull(input);
            return input.get().sortByScoreDesc();
        }, e -> new DataSet<>(), input).name("DataFlow.sort");
    }

    public Operation<DataSet<Row>> readDataSource(String key, Operation<Map<String, String>> extraParamOp) {
        return new ReadDataSource(key, extraParamOp);
    }

    public Operation<DataSet<Row>> readDataSource(String key) {
        return new ReadDataSource(key);
    }

    public Operation<DataSet<Row>> readDataSource(String dataSourceKey, Consumer<Row> consumer) {
        return new ReadDataSource(dataSourceKey, consumer);
    }

    public Operation<DataSet<Row>> splitVdo(Operation<DataSet<Row>> input) {
        return new SplitVdo(input);
    }

    public Operation<DataSet<Row>> splitShow(Operation<DataSet<Row>> input) {
        return new SplitShow(input);
    }


    public Operation<DataSet<Row>> dataSetUnion(Operation<DataSet<Row>>... dataSets) {
        return null;
    }

    public Operation<DataSet<Row>> innerJoin(Operation<DataSet<Row>> left, Operation<DataSet<Row>> right) {
        return new InnerJoin(left, right);
    }

    public Operation<DataSet<Row>> runProcessor(String key) {
        return new RunProcessor(key);
    }

    public Operation<DataSet<Row>> runProcessor(String key, Operation<DataSet<Row>> dependedOp) {
        return new RunProcessor(key, Arrays.asList(dependedOp));
    }

    public Operation<DataSet<Row>> runProcessor(String key, List<Operation<DataSet<Row>>> dependedOps) {
        return new RunProcessor(key, dependedOps);
    }

    public Operation<DataSet<Row>> processRow(Operation<DataSet<Row>> input, Consumer<Row> process) {
        return newOp(ctx -> {
            DataSet<Row> data = input.get();
            data.getData().forEach(process);
            return data;
        }, e -> new DataSet<>(), input).name("DataFlow.processRow");
    }
}