package org.java.plus.dag.core.base.proc;

import java.util.Map;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;

/**
 * @author seven.wxy
 * @date 2018/9/25
 */
public interface Processor extends BaseInterface {
    /**
     * processor method, every request will call this method
     *
     * @param processorContext the processor context,
     *                         contains TPP request url parameters and the processor context parameters
     * @param dataSetMap       multiple [[DataSet]] input, key=processor instance id, value=dataSet
     * @return [[DataSet]] map, the final tpp recommend result will get key=Constants.MAIN_CHAIN_KEY [[DataSet]] in the result map
     */
    Map<String, DataSet<Row>> process(ProcessorContext processorContext, Map<String, DataSet<Row>> dataSetMap);

    /**
     * get processor execute timeout millisecond(ms)
     * @return
     */
    int getProcessorTimeout();
}
