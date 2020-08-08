package org.java.plus.dag.core.operator.sort;

import java.util.Comparator;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.operator.AbstractOperator;
import org.java.plus.dag.core.operator.SortOperator;

/**
 * @author seven.wxy
 * @date 2018/9/26
 */
public class SimpleSortOperator<T extends Row> extends AbstractOperator implements SortOperator<T> {
    @Override
    public DataSet<T> sort(ProcessorContext processorContext, DataSet<T> dataSet, Comparator<T> comparator) {
        return dataSet.sort(comparator);
    }
}
