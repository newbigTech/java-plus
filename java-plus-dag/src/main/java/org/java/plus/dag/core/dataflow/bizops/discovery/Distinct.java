package org.java.plus.dag.core.dataflow.bizops.discovery;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.dataflow.core.Operation;

/**
 * DESCRIPTION:
 *
 * @author jaye
 * @date 2019/3/7
 */
public class Distinct extends Operation<DataSet<Row>> {
    private Operation<DataSet<Row>> inputOp;
    private Function<Row, Object> distinctFieldFunc;
    private Function<Row, Boolean> conditionFunc = row -> true;

    private Function<Throwable, DataSet<Row>> errHanlder = e -> {
        logWarn(String.format("error[%s], return empty dataset", e.getMessage()));
        return new DataSet<>();
    };

    public Distinct(Operation<DataSet<Row>> inputOp, Function<Row, Object> distinctFieldFunc) {
        this.inputOp = inputOp;
        this.distinctFieldFunc = distinctFieldFunc;
        depend(inputOp);
    }

    public Distinct(Operation<DataSet<Row>> inputOp, Function<Row, Object> distinctFieldFunc,
                    Function<Row, Boolean> conditionFunc) {
        this.inputOp = inputOp;
        this.distinctFieldFunc = distinctFieldFunc;
        this.conditionFunc = conditionFunc;
        depend(inputOp);
    }

    @Override
    public DataSet<Row> apply(ProcessorContext ctx) {
        handleErr(errHanlder);
        requireOpNonNull(inputOp);
        requireNonNull(distinctFieldFunc, conditionFunc);
        DataSet<Row> result = new DataSet<>();
        Set<Object> uniqObjects = new HashSet<>();
        inputOp.get().getData().forEach(row -> {
            Object distinctField = distinctFieldFunc.apply(row);
            if (conditionFunc.apply(row) && !uniqObjects.contains(distinctField)) {
                uniqObjects.add(distinctField);
                result.getData().add(row);
            }
        });
        return result;
    }
}