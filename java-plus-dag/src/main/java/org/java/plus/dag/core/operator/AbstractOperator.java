package org.java.plus.dag.core.operator;

import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.proc.AbstractInit;

/**
 * @author seven.wxy
 * @date 2018/9/25
 */
public abstract class AbstractOperator extends AbstractInit implements Operator {
    @Override
    public void doInit(ProcessorConfig processorConfig) {

    }

    protected static void checkNotNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

}
