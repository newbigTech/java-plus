package org.java.plus.dag.core.dataflow.bizops.home_page;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.dataflow.core.Operation;

/**
 * DESCRIPTION:
 *
 * @author jaye
 * @date 2019/3/6
 */
public class DemoOp extends Operation<DataSet<Row>> {
    public DemoOp(Operation<String> dependInputA, Operation<String> dependInputB) {}

    @Override
    public DataSet<Row> apply(ProcessorContext ctx) {
        return null;
    }
}