package org.java.plus.dag.core.base.utils;

import java.util.concurrent.Executor;

//import com.taobao.recommendplatform.protocol.concurrent.SolutionInvoker;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
import org.jetbrains.annotations.NotNull;

/**
 * @author seven.wxy
 * @date 2019/1/7
 */
public class InvokerExecutor implements Executor {
    @Override
    public void execute(@NotNull Runnable command) {
//        SolutionInvoker solutionInvoker = ServiceFactory.getSolutionInvoker().begin(this.getClass().getName());
//        solutionInvoker = solutionInvoker.invoke(String.valueOf(command.hashCode()), () -> {
//            command.run();
//            return null;
//        });
//        solutionInvoker.end();
    }
}
