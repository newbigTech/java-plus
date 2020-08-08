package org.java.plus.dag.core.operator;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;

/**
 * @author seven.wxy
 * @date 2018/9/26
 */
public interface MultiDataSetOperator<T extends Row> extends Operator {
    /**
     * Multi DataSet operator
     * @param processorContext
     * @param dataSetA
     * @param dataSetB
     * @return
     */
    DataSet<T> op(ProcessorContext processorContext, DataSet<T> dataSetA, DataSet<T> dataSetB);
}
