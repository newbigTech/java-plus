package org.java.plus.dag.core.operator;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;

/**
 * @author seven.wxy
 * @date 2018/9/26
 */
public interface ShuffleOperator<T extends Row> extends Operator {
    /**
     * Custom shuffle op
     * @param processorContext
     * @param dataSet
     * @return
     */
    DataSet<T> shuffle(ProcessorContext processorContext, DataSet<T> dataSet);
}
