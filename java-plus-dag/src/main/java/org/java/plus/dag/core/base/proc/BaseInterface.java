package org.java.plus.dag.core.base.proc;

import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.model.ProcessorConfig;

/**
 * @author seven.wxy
 * @date 2018/10/19
 */
public interface BaseInterface {
    /**
     * init method, only call this method on new class instance
     *
     * @param processorConfig kv config json parameter
     */
    void init(ProcessorConfig processorConfig);

    /**
     * get instanceKey
     *
     * @return instanceKey
     */
    String getInstanceKey();

    /**
     * get instanceKey
     *
     * @return
     */
    default ProcessorConfig getProcessorConfig() {
        return ConstantsFrame.EMPTY_CONFIG;
    }

    /**
     * get processor name
     * @return
     */
    default String getName() {
        return this.getClass().getName();
    }

}
