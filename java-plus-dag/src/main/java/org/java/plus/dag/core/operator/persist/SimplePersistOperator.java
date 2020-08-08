package org.java.plus.dag.core.operator.persist;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.operator.AbstractOperator;
import org.java.plus.dag.core.operator.PersistOperator;

/**
 * @author seven.wxy
 * @date 2018/9/26
 */
public class SimplePersistOperator<T extends Row> extends AbstractOperator implements PersistOperator<T> {
    @Override
    public DataSet<T> persist(String key, DataSet<T> dataSet) {
        return dataSet;
    }
}
