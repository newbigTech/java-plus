package org.java.plus.dag.core.operator;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.Row;

/**
 * @author seven.wxy
 * @date 2018/9/26
 */
public interface PersistOperator<T extends Row> extends Operator {
    /**
     * Custom persist op
     * @param key
     * @param dataSet
     * @return
     */
    DataSet<T> persist(String key, DataSet<T> dataSet);
}
