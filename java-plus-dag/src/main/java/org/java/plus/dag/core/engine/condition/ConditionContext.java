package org.java.plus.dag.core.engine.condition;

import lombok.Getter;

/**
 * @author seven.wxy
 * @date 2019/1/30
 */
public class ConditionContext<CONTEXT, OUTPUT> {
    @Getter
    private CONTEXT context;
    @Getter
    private OUTPUT dependedResult;

    public ConditionContext(CONTEXT context, OUTPUT dependedResult) {
        this.context = context;
        this.dependedResult = dependedResult;
    }
}
