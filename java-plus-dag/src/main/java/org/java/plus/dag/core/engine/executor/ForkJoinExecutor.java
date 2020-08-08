package org.java.plus.dag.core.engine.executor;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

/**
 * JDK fork join executor
 *
 * @author jaye
 * @date 2019/3/20
 */
public class ForkJoinExecutor implements DagExecutor {
    private ForkJoinPool executor = ForkJoinPool.commonPool();

    public ForkJoinExecutor() {}

    @Override
    public Future execute(Runnable command) {
        // the return future seems has no effect, when use future.get(), it will return immediately
        return executor.submit(() -> {
            command.run();
            return null;
        });
    }

    @Override
    public String getName() {
        return "ForkJoinExecutor";
    }
}