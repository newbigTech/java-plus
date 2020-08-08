package org.java.plus.dag.core.engine.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Predicate;

//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
import org.java.plus.dag.core.base.constants.TppCounterNames;
import org.java.plus.dag.core.base.utils.CommonMethods;
import org.java.plus.dag.core.base.utils.DagUtil;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.base.utils.ThreadLocalUtils;
import org.java.plus.dag.core.engine.DagEngine;
import org.java.plus.dag.core.engine.DagFunction;
import org.java.plus.dag.core.engine.DagNode;
import org.java.plus.dag.core.engine.DagStruct;
import org.java.plus.dag.core.engine.condition.ConditionContext;
import org.java.plus.dag.core.engine.executor.DagExecutor;
import org.java.plus.dag.core.engine.executor.ForkJoinExecutor;
import org.java.plus.dag.core.engine.executor.TppExecutor;
import org.java.plus.dag.core.engine.executor.WispExecutor;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

/**
 * DESCRIPTION: DagEngine that use queueing execution in thread pool for running rather than CompletableFuture
 *
 * @author jaye
 * @date 2019/3/19
 */
public class DagEngineQueue<CONTEXT> implements DagEngine<CONTEXT> {
    @Getter
    private DagStruct<DagNode> dag = new DagStruct<>();
    private DagExecutor executor;
    private BiFunction mergeFunction;

    @Setter
    private int threadCntPerQuery = 1;
    @Setter
    private int timeout = 300;
    @Setter
    private boolean removeWaitTime;
    @Setter
    private boolean async = false;
    @Setter
    @Getter
    private String parentInstanceKey = StringUtils.EMPTY;
    @Setter
    private Map<String, Predicate<ConditionContext>> conditions = null;
    @Setter
    private boolean skipTimeout;

    private DagExecutor getExecutor() {
        return ThreadLocalUtils.isUseTppThreadPool() ? new TppExecutor() :
            (ThreadLocalUtils.isUseNewWispThreadPool() ? new WispExecutor() : new ForkJoinExecutor());
    }

    public DagEngineQueue() {}

    public DagEngineQueue(String parentInstanceKey) {
        this.parentInstanceKey = parentInstanceKey;
    }

    @Override
    public void setMergeFunction(BiFunction mergeFunction) {
        this.mergeFunction = mergeFunction;
    }

    private BiFunction getMergeFunction() {
        return Objects.isNull(mergeFunction) ? DagUtil.getMergeFunction() : mergeFunction;
    }

    private boolean conditionTest(DagNode node, CONTEXT context, Object dependedResult) {
        if (Objects.isNull(conditions) || !conditions.containsKey(node.getName())) {
            return true;
        }
        return conditions.get(node.getName()).test(new ConditionContext<>(context, dependedResult));
    }

    private boolean needRun(DagStruct<DagNode> dag, DagNode node, CONTEXT context, Object dependedResult) {
        boolean[] isDependOk = {false};
        dag.forEachDependedVertex(node.getName(), depNode -> {
            if (depNode.needRun()) { isDependOk[0] = true; }
        });
        if (dag.getDependCount(node.getName()) <= 0) { isDependOk[0] = true; }
        boolean needRun = isDependOk[0] ? conditionTest(node, context, dependedResult) : false;
        node.setNeedRun(needRun);
        return needRun;
    }

    private Object prepareInput(DagNode node, DagStruct<DagNode> dag, Object input) {
        if (dag.getDependCount(node.getName()) <= 0) {
            return input;
        }
        final Object[] finalResult = {null};
        dag.forEachDependedVertex(node.getName(), depNode ->
            finalResult[0] = Objects.isNull(finalResult[0])
                ? depNode.getResult() : getMergeFunction().apply(finalResult[0], depNode.getResult())
        );
        return finalResult[0];
    }

    private void afterNodeFinished(DagNode node, DagContext<CONTEXT> dagContext) {
        if (node == dagContext.dag.getEndPoint()) {
            dagContext.finishedMsg.complete(new Object());
            return;
        }
        List<DagNode> readyNodes = new ArrayList<>();
        dagContext.dag.forEachFollowingVertex(node.getName(), followNode -> {
            if (0 == followNode.getDependCount().decrementAndGet()) {
                readyNodes.add(followNode);
            }
        });
        if (readyNodes.isEmpty()) { return; }
        for (int i = 0; i < readyNodes.size() - 1; ++i) {
            runNodeAsync(readyNodes.get(i), dagContext);
        }
        runNodeSync(readyNodes.get(readyNodes.size() - 1), dagContext);
    }

    private void runNodeAsync(DagNode node, DagContext<CONTEXT> dagContext) { runNode(node, dagContext, true); }
    private void runNodeSync(DagNode node, DagContext<CONTEXT> dagContext) { runNode(node, dagContext, false); }

    private void runNode(DagNode node, DagContext<CONTEXT> dagContext, boolean async) {
        if (null == node || node.isFinished()) { return; }
        Object depResult = prepareInput(node, dagContext.dag, dagContext.input);
        boolean timeoutSkip = skipTimeout && isTimeout(dagContext);
        if (timeoutSkip) {
//            ServiceFactory.getTPPCounter().countSum(
//                CommonMethods.getCounterKey(TppCounterNames.PROC_SKIP.getCounterName(), parentInstanceKey) + "_" + node.getName(), 1);
            node.setResult(depResult);
            afterNodeFinished(node, dagContext);
            return;
        }

        if (!needRun(dagContext.dag, node, dagContext.context, depResult)) {
            node.setResult(depResult);
            afterNodeFinished(node, dagContext);
            return;
        }
        if (!async || dagContext.threadCntNow.get() >= threadCntPerQuery) {
            node.run(dagContext.dag, dagContext.context, depResult);
            afterNodeFinished(node, dagContext);
            return;
        }
        dagContext.threadCntNow.incrementAndGet();
        executor.execute(() -> {
            node.run(dagContext.dag, dagContext.context, depResult);
            afterNodeFinished(node, dagContext);
            dagContext.threadCntNow.decrementAndGet();
        });
    }

    @Override
    public <INPUT_F, INPUT_T, OUTPUT_F, OUTPUT_T> DagEngine addDependence(String fromFunctionUniqueId,
                                                                          DagFunction<CONTEXT, INPUT_F, OUTPUT_F> from,
                                                                          String toFunctionUniqueId,
                                                                          DagFunction<CONTEXT, INPUT_T,  OUTPUT_T> to) {
        DagNode<CONTEXT, INPUT_F, OUTPUT_F> fromNode = new DagNode<>(fromFunctionUniqueId, from);
        DagNode<CONTEXT, INPUT_T, OUTPUT_T> toNode = new DagNode<>(toFunctionUniqueId, to);
        dag.addVertex(fromNode.getName(), fromNode);
        dag.addVertex(toNode.getName(), toNode);
        dag.addEdge(fromNode.getName(), toNode.getName());
        return this;
    }

    @Override
    public <INPUT, OUTPUT> DagEngine addNode(
        String functionUniqueId, DagFunction<CONTEXT, INPUT, OUTPUT> function) {
        Objects.requireNonNull(functionUniqueId, "functionId is required");
        Objects.requireNonNull(function, "function is required");
        DagNode<CONTEXT, INPUT, OUTPUT> node = new DagNode<>(functionUniqueId, function);
        dag.addVertex(functionUniqueId, node);
        return this;
    }

    private void setupAllNodes() {
        dag.forEachVertex(node -> node.setDependCount(dag.getDependCount(node.getName())));
    }

    @Override
    public boolean initDagStruct() {
        if (!dag.init()) {
            Logger.warn(() -> "dag init failed");
            return false;
        }
        setupAllNodes();
        executor = getExecutor();
        return true;
    }

    @Override
    public <OUTPUT> OUTPUT run() { return run(null, null); }

    public <INPUT, OUTPUT> OUTPUT run(INPUT input) { return run(null, input); }

    @Override
    public <INPUT, OUTPUT> OUTPUT run(CONTEXT context, INPUT input) {
        long start = System.currentTimeMillis();
        DagStruct<DagNode> cloneDag = dag.cloneDag(DagNode::new);
        List<DagNode> startNodes = cloneDag.getStartPoint();
        if (startNodes.isEmpty()) {
            Logger.warn(() -> "start nodes is empty, maybe not init or wrong dag dependency");
            return null;
        }
        DagContext<CONTEXT> dagContext = new DagContext<>(cloneDag, context, input, start);
        executor.execute(() -> {
            for (int i = 0; i < startNodes.size() - 1; ++i) {
                runNodeAsync(startNodes.get(i), dagContext);
            }
            runNodeSync(startNodes.get(startNodes.size() - 1), dagContext);
        });
        long submitCost = System.currentTimeMillis() - start;
        waitForFinished(dagContext, timeout, submitCost);
        OUTPUT result = (OUTPUT)cloneDag.getEndPoint().getResult();
        return Objects.isNull(result) ? (OUTPUT)input : result;
    }

    private void waitForFinished(DagContext<CONTEXT> dagContext, long timeoutExpect, long submitCost) {
        long waitTime = DagUtil.getTimeout(dagContext.startTime, timeout, removeWaitTime);
        try {
            dagContext.finishedMsg.get(waitTime, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
//            ServiceFactory.getTPPCounter().countSum(
//                CommonMethods.getCounterKey(TppCounterNames.PROC_DAG_TIMEOUT.getCounterName(), parentInstanceKey) + "_" + timeoutExpect, 1);
        }
    }

    class DagContext<CONTEXT> {
        DagStruct<DagNode> dag;
        CONTEXT context;
        Object input;
        AtomicInteger threadCntNow;
        CompletableFuture<Object> finishedMsg;
        long startTime;

        DagContext(DagStruct<DagNode> dag, CONTEXT context, Object input, long startTime) {
            this.dag = dag;
            this.context = context;
            this.input = input;
            this.threadCntNow = new AtomicInteger();
            this.startTime = startTime;
            this.finishedMsg = new CompletableFuture();
        }
    }

    private boolean isTimeout(DagContext<CONTEXT> dagContext) {
        return !CommonMethods.disableTimeout() && System.currentTimeMillis() - dagContext.startTime > timeout;
    }
}
