package org.java.plus.dag.core.engine;

import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.apache.commons.lang3.StringUtils;

/**
 * @author seven.wxy
 * @date 2019/2/12
 */
@FunctionalInterface
public interface DagFunction<CONTEXT, INPUT, OUTPUT> {
    /**
     * return function name
     * @return
     */
    default String getFunctionName() {
        return StringUtils.EMPTY;
    }

    /**
     * Function timeout config
     *
     * @return
     */
    default int getTimeoutMs() {
        return ConstantsFrame.PROCESSOR_DEFAULT_TIMEOUT_MS;
    }

    /**
     * Dag engine execute function
     *
     * @param context
     * @param input
     * @param exception
     * @return
     */
    OUTPUT apply(CONTEXT context, INPUT input, Throwable exception);
}

