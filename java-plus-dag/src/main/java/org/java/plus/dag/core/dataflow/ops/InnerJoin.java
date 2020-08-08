package org.java.plus.dag.core.dataflow.ops;

import java.util.function.Function;

import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.dataflow.core.Operation;

/**
 * √Ë ˆ:
 * <p>
 * org.java.plus.dag.frame.dataflow.ops.InnerJoin
 *
 * @author jaye
 * @date 2019/2/23
 * <p>
 * config_start: |org.java.plus.dag.frame.dataflow.ops.InnerJoin||jaye| config_end:
 */
public class InnerJoin extends Operation<DataSet<Row>> {
    private Operation<DataSet<Row>> left;
    private Operation<DataSet<Row>> right;
    private Function<Row, Object> mKeyFunc = Row::getId;

    private InnerJoin() {}

    public InnerJoin(Operation<DataSet<Row>> left, Operation<DataSet<Row>> right) {
        this.left = left;
        this.right = right;
        depend(left, right);
    }

    public InnerJoin(Operation<DataSet<Row>> left, Operation<DataSet<Row>> right, AllFieldName joinField) {
        this.left = left;
        this.right = right;
        mKeyFunc = row -> row.getFieldValue(joinField);
        depend(left, right);
    }

    @Override
    public DataSet<Row> apply(ProcessorContext ctx) {
        requireOpNonNull(left, right);
        DataSet<Row> leftData = left.get();
        DataSet<Row> rightData = right.get();
        return leftData.innerJoin(rightData, mKeyFunc, mKeyFunc);
    }
}