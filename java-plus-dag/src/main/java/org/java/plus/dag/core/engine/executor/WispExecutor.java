package org.java.plus.dag.core.engine.executor;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

//import com.alibaba.glaucus.engine.executor.wisp.WispMultiThreadSolutionExecutor;
//import com.alibaba.tpp.concurrent.threadfactory.NamedThreadFactory;

/**
 * Wisp executor
 * @author seven.wxy
 * @date 2019/6/25
 */
public class WispExecutor implements DagExecutor {
//    private static WispMultiThreadSolutionExecutor INSTANCE = new WispMultiThreadSolutionExecutor(8, 1000,
//        new NamedThreadFactory("yk-wisp"),
//        (r) -> {
//            throw new RejectedExecutionException(
//                "Task " + r.toString() + " rejected from yk-wisp WispMultiThreadSolutionExecutor");
//        });
    
    private static ThreadPoolExecutor INSTANCE =new ThreadPoolExecutor(10, 10, 10, null, null);
    @Override
    public Future execute(Runnable command) {
        return INSTANCE.submit(command);
    }

    @Override
    public String getName() {
        return "WispExecutor";
    }
}
