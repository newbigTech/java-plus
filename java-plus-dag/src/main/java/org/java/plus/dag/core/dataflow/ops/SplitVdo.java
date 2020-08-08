package org.java.plus.dag.core.dataflow.ops;

import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.dataflow.core.Operation;

/**
 * ����:
 * <p>
 org.java.plus.dagon.frame.dataflow.ops.SplitVdo
 *
 * @author jaye
 * @date 2019/2/20
 * <p>
 * config_start:org.java.plus.dagon.frame.dataflow.ops.SplitVdo||jaye| config_end:
 */
public class SplitVdo extends Operation<DataSet<Row>> {
    private Operation<DataSet<Row>> input;

    private SplitVdo() {}

    public SplitVdo(Operation<DataSet<Row>> input) {
        this.input = input;
        depend(input);
    }

    @Override
    public DataSet<Row> apply(ProcessorContext ctx) {
        DataSet<Row> result = new DataSet<>();
        input.get().getData().forEach(row -> {
            if (row.getFieldValue(AllFieldName.type).equals("1"))  { result.getData().add(row); }
        });
        return result;
    }
}