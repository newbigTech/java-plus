package org.java.plus.dag.core.dataflow.core;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.dataflow.Global;
import org.java.plus.dag.core.engine.DagFunction;

/**
 * ����:
 * <p>
 * org.java.plus.dag.frame.dataflow.core.Operation
 *
 * @author jaye
 * @date 2019/2/20
 * <p>
 * config_start: |org.java.plus.dag.frame.dataflow.core.Operation||jaye| config_end:
 */
public abstract class Operation<T> implements DagFunction<ProcessorContext, Object, Object> {
    private String mName = this.toString();
    private List<Operation<?>> mDependedOps = new ArrayList<>();

    private T mResult;
    private boolean mIsFinished = false;
    private boolean mIsException = false;

    private String mCurProcessorName = "";

    private Function<Throwable, T> mErrHandler = e -> {
        logWarn("Exception occurred, msg: " + e.getMessage());
        return null;
    };

    private boolean isFinished() { return mIsFinished; }
    private boolean isException() { return mIsException; }

    private void setResult(T result) {
        mResult = result;
        mIsFinished = true;
    }

    final void setCurProcessorName(String processor) { mCurProcessorName = processor; }
    protected final String getCurProcessorName() { return mCurProcessorName; }

    /**
     * give a name to this op
     */
    public final Operation<T> name(String name) {
        setName(name);
        return this;
    }

    public final Operation<T> handleErr(Function<Throwable, T> handler) {
        mErrHandler = handler;
        return this;
    }

    public final Operation<T> depend(List<Operation> ops) {
        if (null == ops || ops.isEmpty()) {
            return this;
        }
        if (null == mDependedOps) { mDependedOps = new ArrayList<>(ops.size()); }
        for (Operation op : ops) {
            if (null != op) { mDependedOps.add(op); }
        }
        return this;
    }

    public final Operation<T> depend(Operation... ops) {
        return depend(Arrays.asList(ops));
    }

    public final List<Operation<?>> getDependedOps() { return mDependedOps; }

    public final String getName() { return mName; }

    public final void setName(String name) {
        if (null != name) { mName = name; }
    }

    @Override
    public final Object apply(ProcessorContext ctx, Object noUsed, Throwable exception) {
        T opResult = null;
        try {
            if (null != exception) {
                logWarn("the previous op exception occurred: " + exception.getMessage());
            }
            opResult = apply(ctx);
        } catch (Exception e) {
            mIsException = true;
            try {
                opResult = mErrHandler.apply(e);
            } catch (Exception exAgain) {
                logWarn("exception in err handler: " + exAgain.getMessage());
            }
        }
        setResult(opResult);
        return null;
    }

    public final T get() { return mResult; }

    protected final void requireOpNonNull(Operation... ops) { requireOpNonNull(null, ops); }

    protected final void requireOpNonNull(Supplier<T> onErr, Operation... ops) {
        Function<Throwable, T> oldHandler = mErrHandler;
        if (null != onErr) { mErrHandler = e -> onErr.get(); }
        for (Operation op : ops) {
            Objects.requireNonNull(ops);
            Objects.requireNonNull(op.get());
        }
        mErrHandler = oldHandler;
    }

    protected final boolean isNullOp(Operation... ops) {
        return Global.isOpsNull(ops);
    }

    protected final void requireNonNull(Object... objs) {
        Global.requireNonNull(objs);
    }

    protected final boolean isNull(Object... objs) {
        return Global.isNull(objs);
    }

    protected final void logInfo(String log) {
        Logger.info(() -> MessageFormat.format("{0}[{1}]: {2}", getName(), mCurProcessorName, log));
    }

    protected final void logWarn(String log) {
        Logger.warn(() -> MessageFormat.format("{0}[{1}]: {2}", getName(), mCurProcessorName, log));
    }

    protected final void logErr(Throwable t, String log) {
        Logger.error(() -> MessageFormat.format("{0}[{1}]: {2}", getName(), mCurProcessorName, log), t);
    }

    public T eval(ProcessorContext ctx, String curProcessorName) {
        mCurProcessorName = curProcessorName;
        if (!getDependedOps().isEmpty()) {
            for (Operation op : getDependedOps()) {
                if (!op.isFinished()) {
                    op.eval(ctx, mCurProcessorName);
                }
            }
        }
        apply(ctx, null, null);
        return mResult;
    }

    public T eval(ProcessorContext ctx, String curProcessorName, Supplier<T> defaultVal) {
        T result = eval(ctx, curProcessorName);
        if (null == result && null != defaultVal) { result = defaultVal.get(); }
        return result;
    }

    public T eval(String curProcessorName) { return eval(new ProcessorContext(), curProcessorName); }

    public T eval(String curProcessorName, Supplier<T> defaultVal) {
        return eval(new ProcessorContext(), curProcessorName, defaultVal);
    }

    /**
     * this is the only method we should override to generate op output
     * @param ctx the context
     */
    public abstract T apply(ProcessorContext ctx);
}