package org.java.plus.dag.core.engine.executor;

//import com.taobao.recommendplatform.protocol.concurrent.SolutionInvoker;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;

import java.util.concurrent.Future;

/**
 * Tpp SolutionInvoker executor
 */
public class TppExecutor implements DagExecutor {
    @Override
    public Future execute(Runnable command) {
        String name = String.valueOf(command.hashCode());
//        SolutionInvoker solutionInvoker = ServiceFactory.getSolutionInvoker().begin(this.getClass().getName());
//        solutionInvoker = solutionInvoker.invoke(name, () -> {
//            command.run();
//            return null;
//        });
//        solutionInvoker.end();
        return null;//solutionInvoker.getAsync(name);
    }

    @Override
    public String getName() {
        return "TppExecutor";
    }
}