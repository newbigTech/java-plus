package org.java.plus.dag.core.base.model;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;

//import com.alibaba.glaucus.engine.executor.wisp.WispMultiThreadSolutionExecutor;
//import com.alibaba.tpp.concurrent.threadfactory.NamedThreadFactory;

import org.java.plus.dag.core.base.utils.Debugger;
import org.java.plus.dag.core.base.utils.InvokerExecutor;

/**
 * @author seven.wxy
 * @date 2018/12/4
 */
public class TaskExecutor {
    public static final Executor SINGLE_THREAD = getSingleThreadExecutor();
    public static final Executor TPP_THREAD_POOL = getTppThreadPoolExecutor();
    public static final Executor FORK_JOIN_POOL = getForkJoinPoolExecutor();
    public static final Executor NEW_WISP_JOIN_POOL = getWispThreadExecutor();
    public static final String POOL_NAME = "mwisp";

    private static class TppInvokerExecutor {
        private static Executor INSTANCE = new InvokerExecutor();
    }

    private static class WispExecutor{
        private static Executor INSTANCE =getForkJoinPoolExecutor();
        		
//        		new WispMultiThreadSolutionExecutor(8, 1000,
//            new NamedThreadFactory(POOL_NAME),
//            (r) -> {
//                throw new RejectedExecutionException("Task " + r.toString() + " rejected from yk-wisp WispMultiThreadSolutionExecutor");
//            });
        
    }

    private static Executor getSingleThreadExecutor() {
        return (runnable) -> runnable.run();
    }

    private static Executor getWispThreadExecutor() {
        return WispExecutor.INSTANCE;
    }

    private static Executor getTppThreadPoolExecutor() {
        return TppInvokerExecutor.INSTANCE;
    }

    private static Executor getForkJoinPoolExecutor() {
        return ForkJoinPool.commonPool();
    }

    public static Executor findExecutor(boolean multiThreadExecute, boolean useTppThreadPool, boolean useNewWispThreadPool) {
        Executor executor = Objects.isNull(SINGLE_THREAD) ? getSingleThreadExecutor() : SINGLE_THREAD;
        if (!Debugger.isLocal() && multiThreadExecute) {
            if (useTppThreadPool) {
                if (useNewWispThreadPool) {
                    executor = Objects.isNull(NEW_WISP_JOIN_POOL) ? getTppThreadPoolExecutor() : NEW_WISP_JOIN_POOL;
                } else {
                    executor = Objects.isNull(TPP_THREAD_POOL) ? getTppThreadPoolExecutor() : TPP_THREAD_POOL;
                }
                if (executor == null) {
                    executor = Objects.isNull(FORK_JOIN_POOL) ? getForkJoinPoolExecutor() : FORK_JOIN_POOL;
                }
            } else {
                executor = Objects.isNull(FORK_JOIN_POOL) ? getForkJoinPoolExecutor() : FORK_JOIN_POOL;
            }
        }
        return executor;
    }
}
