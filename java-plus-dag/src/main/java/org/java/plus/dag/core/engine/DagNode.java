package org.java.plus.dag.core.engine;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.java.plus.dag.core.base.utils.Logger;
import lombok.Getter;
import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * Dag node
 * @author jaye
 * @date 2019/3/16
 */
public class DagNode<CONTEXT, INPUT, OUTPUT> {
    /**
     * dag node name, node business name
     */
    private String name;
    /**
     * node run function
     */
    @Getter
    private DagFunction<CONTEXT, INPUT, OUTPUT> function;
    /**
     * each DagNode will save it result and set finished state after run, so the other nodes can fetch the result directly
     */
    private boolean isFinished;
    /**
     * node run result
     */
    private OUTPUT result;
    /**
     * node need run or not, condition node will set this prop
     */
    private boolean needRun = true;

    /**
     * this node depend node count
     */
    private AtomicInteger dependCount = new AtomicInteger();

    public DagNode(String name, DagFunction<CONTEXT, INPUT, OUTPUT> function) {
        this.name = name;
        this.function = function;
    }

    public DagNode(DagNode<CONTEXT, INPUT, OUTPUT> node) {
        this.name = node.name;
        this.function = node.function;
        this.dependCount.set(node.dependCount.get());
    }

    public void setResult(OUTPUT result) {
        this.result = result;
        this.isFinished = true;
    }

    public void setNeedRun(boolean needRun) { this.needRun = needRun; }

    public boolean needRun() { return this.needRun; }

    public boolean isFinished() { return isFinished; }

    public String getName() { return name; }

    public Object getResult() { return result; }

    public void setDependCount(int count) {
        dependCount.set(count);
    }

    public AtomicInteger getDependCount() { return dependCount; }

    public void run(DagStruct<DagNode> dag, CONTEXT context, INPUT input) {
        Objects.requireNonNull(dag);
        Objects.requireNonNull(function);
        if (isFinished || !needRun) { return; }
        try {
            result = function.apply(context, input, null);
        } catch (Exception e) {
            Logger.warn(() -> String.format("run node[%s] failed, exception[%s]", name, ExceptionUtils.getStackTrace(e)));
        } finally {
            isFinished = true;
        }
    }
}