package org.java.plus.dag.core.operator.extend;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.operator.AbstractOperator;
import org.java.plus.dag.core.operator.SingleDataSetOperator;

/**
 * @author seven.wxy
 * @date 2019/7/10
 */
public class SimpleSingleOperator<T extends Row> extends AbstractOperator implements SingleDataSetOperator<T> {
    @Override
    public DataSet<T> op(ProcessorContext processorContext, DataSet<T> dataSet) {
        return dataSet;
    }
}
