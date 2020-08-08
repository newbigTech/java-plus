package org.java.plus.dag.core.base.proc;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;

/**
 * @author seven.wxy
 * @date 2018/9/26
 */
public class BaseProcessor extends AbstractBaseProcessor {
    @Override
    public void doInit(ProcessorConfig processorConfig) {

    }

    @Override
    public DataSet<Row> doProcess(ProcessorContext processorContext, DataSet<Row> dataSet) {
        return dataSet;
    }
}
