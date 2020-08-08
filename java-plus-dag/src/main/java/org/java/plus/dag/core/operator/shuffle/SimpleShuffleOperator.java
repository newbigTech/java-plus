package org.java.plus.dag.core.operator.shuffle;

import java.util.concurrent.ThreadLocalRandom;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.FrameCollectionUtils;
import org.java.plus.dag.core.operator.AbstractOperator;
import org.java.plus.dag.core.operator.ShuffleOperator;

/**
 * @author seven.wxy
 * @date 2018/9/26
 */
public class SimpleShuffleOperator<T extends Row> extends AbstractOperator implements ShuffleOperator<T> {
    @Override
    public DataSet<T> shuffle(ProcessorContext processorContext, DataSet<T> dataSet) {
        FrameCollectionUtils.shuffle(dataSet.getData(), ThreadLocalRandom.current());
        return DataSet.toDS(dataSet.getData());
    }
}
