package org.java.plus.dag.core.operator;

import java.util.Comparator;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;

/**
 * @author seven.wxy
 * @date 2018/9/26
 */
public interface SortOperator<T extends Row> extends Operator {
    /**
     * Custom sort op
     * @param processorContext
     * @param dataSet
     * @param comparator
     * @return
     */
    DataSet<T> sort(ProcessorContext processorContext, DataSet<T> dataSet, Comparator<T> comparator);
}
