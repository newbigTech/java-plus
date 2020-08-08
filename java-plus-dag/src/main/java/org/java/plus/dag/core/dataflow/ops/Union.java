package org.java.plus.dag.core.dataflow.ops;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.dataflow.core.Operation;

/**
 * √Ë ˆ: Operation that simply union all input to a Object org.java.plus.dag.frame.dataflow.ops.Union
 *
 * @author jaye
 * @date 2019/2/20
 * <p>
 * config_start: |org.java.plus.dag.frame.dataflow.ops.Union|Operation that simply union all
 * input to map|jaye| config_end:
 */
public class Union extends Operation<DataSet<Row>> {
    private Union() {}

    @SafeVarargs
    public Union(Operation<DataSet<Row>>... ops) {
        depend(ops);
    }

    @Override
    public DataSet<Row> apply(ProcessorContext ctx) {
        DataSet<Row> resultDs = new DataSet<>();
        for (Operation op : getDependedOps()) {
            try {
                resultDs = resultDs.unionAll((DataSet<Row>)op.get());
            } catch (Exception e) {
                logWarn("Union: union dataSet failed");
                return null;
            }
        }
        return resultDs;
    }
}