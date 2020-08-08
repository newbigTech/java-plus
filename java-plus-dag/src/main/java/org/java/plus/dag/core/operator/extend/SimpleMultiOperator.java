package org.java.plus.dag.core.operator.extend;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.operator.AbstractOperator;
import org.java.plus.dag.core.operator.MultiDataSetOperator;

/**
 * @author seven.wxy
 * @date 2019/7/10
 */
public class SimpleMultiOperator<T extends Row> extends AbstractOperator implements MultiDataSetOperator<T> {
    @Override
    public DataSet<T> op(ProcessorContext processorContext, DataSet<T> dataSetA, DataSet<T> dataSetB) {
        return dataSetA;
    }
}
