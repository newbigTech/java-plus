package org.java.plus.dag.core.dataflow.core;

import java.util.List;
import java.util.function.Supplier;

import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.proc.Processor;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.dataflow.DataFlow;
import org.java.plus.dag.core.engine.future.DagEngineFuture;

/**
 * ����: DataSet Flow org.java.plus.dag.frame.dataflow.DataFlow
 *
 * @author jaye
 * @date 2019/2/20
 * <p>
 * config_start: |org.java.plus.dag.frame.dataflow.DataFlow|DataSet Flow|jaye| config_end:
 */
public class DataFlowCore extends DataFlow {
    private DagEngineFuture<ProcessorContext> mEngine = new DagEngineFuture<>();
    private ProcessorContext mCtx = new ProcessorContext();
    private String mCurProcessorName = "";

    private boolean mAsync = false;
    private int mTimeoutMs = 500;

    public static DataFlow getInstance(Processor processor) {
        return getInstance(null, processor);
    }

    public static DataFlow getInstance(ProcessorContext ctx, Processor processor) {
        DataFlowCore df = new DataFlowCore(ctx);
        if (null != processor) {
            df.mCurProcessorName = processor.getInstanceKey();
        }
        return df;
    }

    private DataFlowCore(ProcessorContext ctx) {
        if (null != ctx) { mCtx = ctx; }
        mEngine.setMergeFunction((dataA, dataB) -> null);
    }

    @Override
    public void reset() { mEngine = new DagEngineFuture<>(); }

    public void setAsync(boolean async) { mAsync = true; }
    public void setTimeout(int timeoutMs) { mTimeoutMs = timeoutMs; }

    private void addDependencyFor(Operation<?> op) {
        List<Operation<?>> dependedOps = op.getDependedOps();
        dependedOps.forEach(depOp -> {
            depOp.setCurProcessorName(mCurProcessorName);
            addDependencyFor(depOp);
            mEngine.addDependence(depOp.getName(), depOp, op.getName(), op);
        });
    }

    @Override
    public <T> T run(Operation<T> op) {
        op.setCurProcessorName(mCurProcessorName);
        if (op.getDependedOps().isEmpty()) {
            mEngine.addNode(op.getName(), op);
        } else {
            addDependencyFor(op);
        }
        if (!mEngine.initDagStruct()) {
            Logger.warn(() -> String.format("[%s]init dag struct failed", mCurProcessorName));
            return null;
        }
        mEngine.setAsync(mAsync);
        mEngine.setTimeout(mTimeoutMs);
        try {
            mEngine.run(mCtx, null);
        } catch (Exception e) {
            Logger.warn(() -> String.format("[%s]DagEngineExecutor run failed: " + e.getMessage(), mCurProcessorName));
            return null;
        }
        return op.get();
    }

    @Override
    public <T> T run(Operation<T> op, Supplier<T> defaultResult) {
        T result = run(op);
        if (null == result) { result = defaultResult.get(); }
        return result;
    }
    @Override
    public <T> T eval(Operation<T> op) { return op.eval(mCtx, mCurProcessorName); }
    @Override
    public <T> T eval(Operation<T> op, Supplier<T> defaultVal) { return op.eval(mCtx, mCurProcessorName, defaultVal); }
}