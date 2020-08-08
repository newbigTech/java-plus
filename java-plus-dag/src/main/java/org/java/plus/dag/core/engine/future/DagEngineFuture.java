package org.java.plus.dag.core.engine.future;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.java.plus.dag.core.base.utils.DagUtil;
import org.java.plus.dag.core.base.utils.FutureUtil;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.engine.DagEngine;
import org.java.plus.dag.core.engine.DagFunction;
import org.java.plus.dag.core.engine.DagStruct;
import org.java.plus.dag.core.engine.condition.ConditionContext;
import org.java.plus.dag.core.engine.DagNode;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Base on CompletableFuture dag executor engine
 *
 * @param <CONTEXT> context type
 * @author seven.wxy
 * @date 2019/1/30
 */
public class DagEngineFuture<CONTEXT> implements DagEngine<CONTEXT> {
    private static final int START_INDEX = 0;
    private static final int SECOND_INDEX = 1;
    private Supplier<Map> concurrentMapSupplier = () -> Maps.newConcurrentMap();
    private Supplier<Map> mapSupplier = () -> Maps.newHashMap();

    @Setter
    @Getter
    private String parentInstanceKey = StringUtils.EMPTY;
    @Setter
    private int timeout;
    @Setter
    private boolean removeWaitTime;
    @Setter
    private boolean async;
    @Setter
    @Getter
    private int threadCntPerQuery;
    /**
     * Condition predicate map, key=functionUniqueId, value=Predicate
     */
    @Setter
    private Map<String, Predicate<ConditionContext>> conditions = concurrentMapSupplier.get();

    /**
     * Parallel result merge function
     */
    private BiFunction mergeFunction;

    /**
     * Dag struct
     */
    @Getter
    private DagStruct<DagNode> dag = new DagStruct<>();

    @Setter
    private boolean skipTimeout;

    /**
     * Dag struct is init or not
     */
    private volatile Boolean init;

    public DagEngineFuture() {}

    public DagEngineFuture(String parentInstanceKey) {
        this.parentInstanceKey = parentInstanceKey;
    }

    /**
     * Add function node to dag, no dependence, functionUniqueId will use function.hashCode
     *
     * @param function to add function
     * @param <INPUT>  input type
     * @param <OUTPUT> output type
     * @return current {@link DagEngineFuture} object
     */
    public <INPUT, OUTPUT> DagEngineFuture addNode(DagFunction<CONTEXT, INPUT, OUTPUT> function) {
        Objects.requireNonNull(function, "function is required");
        return addNode(String.valueOf(function.hashCode()), function);
    }

    /**
     * Add function node with unique id to dag, no dependence
     *
     * @param functionUniqueId function node unique id
     * @param function         to add function
     * @param <INPUT>          input type
     * @param <OUTPUT>         output type
     * @return current {@link DagEngineFuture} object
     */
    @Override
    public <INPUT, OUTPUT> DagEngineFuture addNode(String functionUniqueId,
                                                   DagFunction<CONTEXT, INPUT, OUTPUT> function) {
        Objects.requireNonNull(functionUniqueId, "functionId is required");
        Objects.requireNonNull(function, "function is required");
        DagNode<CONTEXT, INPUT, OUTPUT> node = new DagNode<>(functionUniqueId, function);
        dag.addVertex(functionUniqueId, node);
        return this;
    }

    /**
     * Add node dependence to dag, functionUniqueId will use function.hashCode
     *
     * @param from       from node function
     * @param to         to node function
     * @param <INPUT_F>  from node input type
     * @param <INPUT_T>  to node input type
     * @param <OUTPUT_F> from node output type
     * @param <OUTPUT_T> to node output type
     * @return current {@link DagEngineFuture} object
     */
    public <INPUT_F, INPUT_T, OUTPUT_F, OUTPUT_T> DagEngineFuture addDependence(
        DagFunction<CONTEXT, INPUT_F, OUTPUT_F> from,
        DagFunction<CONTEXT, INPUT_T, OUTPUT_T> to) {
        Objects.requireNonNull(from, "from Node is required");
        Objects.requireNonNull(to, "to Node is required");
        return addDependence(String.valueOf(from.hashCode()), from, String.valueOf(to.hashCode()), to);
    }

    /**
     * Add node with unique id dependence to dag
     *
     * @param fromFunctionUniqueId from node unique id
     * @param from                 from node function
     * @param toFunctionUniqueId   to node unique id
     * @param to                   to node function
     * @param <INPUT_F>            from node input type
     * @param <INPUT_T>            to node input type
     * @param <OUTPUT_F>           from node output type
     * @param <OUTPUT_T>           to node output type
     * @return current {@link DagEngineFuture} object
     */
    @Override
    public <INPUT_F, INPUT_T, OUTPUT_F, OUTPUT_T> DagEngineFuture addDependence(String fromFunctionUniqueId,
                                                                                DagFunction<CONTEXT, INPUT_F, OUTPUT_F> from,
                                                                                String toFunctionUniqueId,
                                                                                DagFunction<CONTEXT, INPUT_T, OUTPUT_T> to) {
        Objects.requireNonNull(fromFunctionUniqueId, "from Node id is required");
        Objects.requireNonNull(from, "from Node is required");
        Objects.requireNonNull(toFunctionUniqueId, "to Node id is required");
        Objects.requireNonNull(to, "to Node is required");
        DagNode<CONTEXT, INPUT_F, OUTPUT_F> fromNode = new DagNode<>(fromFunctionUniqueId, from);
        DagNode<CONTEXT, INPUT_T, OUTPUT_T> toNode = new DagNode<>(toFunctionUniqueId, to);
        dag.addVertex(fromFunctionUniqueId, fromNode);
        dag.addVertex(toFunctionUniqueId, toNode);
        dag.addEdge(fromFunctionUniqueId, toFunctionUniqueId);
        return this;
    }

    /**
     * Add condition node to dag, functionUniqueId will use function.hashCode
     *
     * @param predicate predicate function
     * @param function  function node
     * @param <INPUT>   input type
     * @param <OUTPUT>  output type
     * @return current {@link DagEngineFuture} object
     */
    public <INPUT, OUTPUT> DagEngineFuture addCondition(Predicate<ConditionContext> predicate,
                                                        DagFunction<CONTEXT, INPUT, OUTPUT> function) {
        Objects.requireNonNull(predicate, "predicate is required");
        Objects.requireNonNull(function, "function is required");
        return addCondition(predicate, String.valueOf(function.hashCode()), function);
    }

    /**
     * Add condition node with unique id to dag
     *
     * @param predicate        predicate function
     * @param functionUniqueId function node unique id
     * @param function         function node
     * @param <INPUT>          input type
     * @param <OUTPUT>         output type
     * @return current {@link DagEngineFuture} object
     */
    public <INPUT, OUTPUT> DagEngineFuture addCondition(Predicate<ConditionContext> predicate,
                                                        String functionUniqueId,
                                                        DagFunction<CONTEXT, INPUT, OUTPUT> function) {
        Objects.requireNonNull(predicate, "predicate is required");
        Objects.requireNonNull(functionUniqueId, "functionId is required");
        Objects.requireNonNull(function, "function is required");
        addNode(functionUniqueId, function);
        return addCondition(predicate, functionUniqueId);
    }

    /**
     * Add condition node unique id to dag, the function node must be add to dag in advance
     *
     * @param predicate        predicate function
     * @param functionUniqueId function node unique id
     * @return current {@link DagEngineFuture} object
     */
    public DagEngineFuture addCondition(Predicate<ConditionContext> predicate, String functionUniqueId) {
        Objects.requireNonNull(predicate, "predicate is required");
        Objects.requireNonNull(functionUniqueId, "functionId is required");
        conditions.put(functionUniqueId, predicate);
        return this;
    }

    /**
     * Run dag graph
     *
     * @param <OUTPUT> output type
     * @return output data
     * @throws Exception
     */
    @Override
    public <OUTPUT> OUTPUT run() throws Exception {
        return run(null);
    }

    /**
     * Run dag graph with input data and context
     *
     * @param context  execute context
     * @param input    init input
     * @param <INPUT>  input type
     * @param <OUTPUT> output type
     * @return output data
     * @throws Exception
     */
    @Override
    public <INPUT, OUTPUT> OUTPUT run(CONTEXT context, INPUT input) throws Exception {
        long start = System.currentTimeMillis();
        OUTPUT result = null;
        Boolean isInit = initDagStruct();
        long initDagCost = System.currentTimeMillis() - start;
        long startGetFuture = System.currentTimeMillis();
        if (BooleanUtils.isTrue(isInit)) {
            Map<String, Boolean> conditionResult = concurrentMapSupplier.get();
            Map<String, CompletableFuture> futureMap = Maps.newHashMap();
            FutureContext futureContext = new FutureContext();
            CompletableFuture<OUTPUT> future = getFuture(dag.getEndPointKey(), conditionResult, futureMap, input, context, futureContext);
            long getFutureCost = System.currentTimeMillis() - startGetFuture;
            if (Objects.nonNull(future)) {
                long waitTime = timeout;
                try {
                    waitTime = DagUtil.getTimeout(start, timeout, removeWaitTime);
                    result = future.get(waitTime, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    future.cancel(true);
                    Logger.onlineWarn(parentInstanceKey
                        + ",future dag expect timeout:" + timeout
                        + ",init dag cost:" + initDagCost
                        + ",get future cost:" + getFutureCost
                        + ",actual get wait:" + waitTime
                        + ",actual cost:" + (System.currentTimeMillis() - start));
                }
            }
        }
        return Objects.isNull(result) ? (OUTPUT)input : result;
    }

    /**
     * Run dag graph with input data
     *
     * @param input    init input
     * @param <INPUT>  input type
     * @param <OUTPUT> output type
     * @return output data
     * @throws Exception
     */
    public <INPUT, OUTPUT> OUTPUT run(INPUT input) throws Exception {
        return run(null, input);
    }

    /**
     * init dag struct, call this method before {@link #run(Object, Object)}
     *
     * @return is init success or not
     */
    @Override
    public boolean initDagStruct() {
        if (Objects.isNull(init)) {
            synchronized (this) {
                if (Objects.isNull(init)) {
                    init = dag.init();
                }
            }
        }
        return init;
    }

    /**
     * Merge parallel result function, OUTPUT_A and OUTPUT_B not ensure order which {@link #addDependence}
     * Suggest the same type result to merge, if not, use Map type return
     *
     * @param mergeFunction merge function
     * @param <OUTPUT_A>    type of output one
     * @param <OUTPUT_B>    type of output another
     * @param <OUTPUT>      type of merge result
     */
    @Override
    public <OUTPUT_A, OUTPUT_B, OUTPUT> void setMergeFunction(BiFunction<OUTPUT_A, OUTPUT_B, OUTPUT> mergeFunction) {
        this.mergeFunction = mergeFunction;
    }

    private <INPUT, OUTPUT> CompletableFuture<OUTPUT> getFuture(String curKey,
                                                                Map<String, Boolean> conditionResult,
                                                                Map<String, CompletableFuture> futureMap,
                                                                INPUT input, CONTEXT context, FutureContext futureContext) {
        if (futureMap.containsKey(curKey)) {
            return futureMap.get(curKey);
        }
        DagNode curNode = dag.getVertex(curKey);
        if (Objects.isNull(curNode)) {
            return null;
        }
        List<String> dependedKeyList = dag.getDependedKey(curKey);
        List<CompletableFuture<OUTPUT>> dependedFutureList = Lists.newArrayList();
        for (String depKey : dependedKeyList) {
            CompletableFuture<OUTPUT> future = getFuture(depKey, conditionResult, futureMap, input, context, futureContext);
            if (Objects.isNull(future)) {
                continue;
            }
            dependedFutureList.add(future);
        }
        if (CollectionUtils.isEmpty(dependedFutureList)) {
            dependedFutureList.add(inputAsOutputFuture(input));
        }
        CompletableFuture<OUTPUT> allDepended =
            createParallelTasks(() -> FutureUtil.getExecutor(), async, dependedFutureList);

        CompletableFuture resultFuture;
        if (async && !useCurrentThread(dependedKeyList) && futureContext.threadCount.get() < threadCntPerQuery) {
            futureContext.threadCount.incrementAndGet();
            resultFuture = allDepended.handleAsync(getHandleFunction(curKey, curNode, conditionResult, context, futureContext, true), FutureUtil.getExecutor());
        } else {
            resultFuture = allDepended.handle(getHandleFunction(curKey, curNode, conditionResult, context, futureContext, false));
        }
        futureMap.put(curKey, resultFuture);
        return resultFuture;
    }

    private <INPUT, OUTPUT> BiFunction<INPUT, Throwable, OUTPUT> getHandleFunction(String curKey,
                                                                                   DagNode curNode,
                                                                                   Map<String, Boolean> conditionResult,
                                                                                   CONTEXT context, FutureContext futureContext,
                                                                                   boolean async) {
        return (INPUT input, Throwable exception) -> {
            if (!conditionResult.containsKey(curKey)) {
                conditionResult.put(curKey, needRun(curKey, conditionResult, input, context));
            }
            if (BooleanUtils.isNotTrue(conditionResult.get(curKey))) {
                return (OUTPUT)input;
            }
            try {
                DagFunction<CONTEXT, INPUT, OUTPUT> function = curNode.getFunction();
                return function.apply(context, input, exception);
            } finally {
                if (async) {
                    futureContext.threadCount.decrementAndGet();
                }
            }
        };
    }

    private <INPUT> boolean needRun(String curKey, Map<String, Boolean> conditionResult, INPUT dependedResult,
                                    CONTEXT context) {
        boolean isDeptOk = false;
        List<String> dependedKeyList = dag.getDependedKey(curKey);
        if (CollectionUtils.isEmpty(dependedKeyList)) {
            isDeptOk = true;
        } else {
            for (String dpk : dependedKeyList) {
                if (conditionResult.containsKey(dpk) && BooleanUtils.isTrue(conditionResult.get(dpk))) {
                    isDeptOk = true;
                    break;
                }
            }
        }
        if (!isDeptOk) {
            return false;
        }
        return conditionTest(curKey, dependedResult, context);
    }

    private <INPUT> boolean conditionTest(String curKey, INPUT dependedResult, CONTEXT context) {
        if (Objects.isNull(conditions) || !conditions.containsKey(curKey)) {
            return true;
        }
        return conditions.get(curKey).test(new ConditionContext(context, dependedResult));
    }

    private <OUTPUT> CompletableFuture<OUTPUT> createParallelTasks(Supplier<Executor> supplier, boolean async,
                                                                   List<CompletableFuture<OUTPUT>> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return emptyFuture();
        }
        if (tasks.size() == 1) {
            return tasks.get(START_INDEX);
        }
        CompletableFuture<OUTPUT> task = tasks.get(START_INDEX);
        for (int i = SECOND_INDEX; i < tasks.size(); i++) {
            if (async) {
                task = task.thenCombineAsync(tasks.get(i), (resultA, resultB) -> mergeParallelResult(resultA, resultB),
                    supplier.get());
            } else {
                task = task.thenCombine(tasks.get(i), (resultA, resultB) -> mergeParallelResult(resultA, resultB));
            }
        }
        return task;
    }

    private <OUTPUT_A, OUTPUT_B, OUTPUT> OUTPUT mergeParallelResult(OUTPUT_A resultA, OUTPUT_B resultB) {
        BiFunction<OUTPUT_A, OUTPUT_B, OUTPUT> mergeFunc = getMergeFunction();
        return mergeFunc.apply(resultA, resultB);
    }

    private <OUTPUT> CompletableFuture<OUTPUT> emptyFuture() {
        return CompletableFuture.completedFuture((OUTPUT)mapSupplier.get());
    }

    private <INPUT, OUTPUT> CompletableFuture<OUTPUT> inputAsOutputFuture(INPUT input) {
        return CompletableFuture.completedFuture((OUTPUT)(Objects.isNull(input) ? mapSupplier.get() : input));
    }

    private boolean useCurrentThread(List<String> dependedKeyList) {
        // start point must use async, the chain can be control timeout
        if (dependedKeyList.size() <= 0) {
            return false;
        }
        for (String deptKey : dependedKeyList) {
            if (dag.getFollowingKey(deptKey).size() > 1) {
                return false;
            }
        }
        return true;
    }

    private <OUTPUT_A, OUTPUT_B, OUTPUT> BiFunction<OUTPUT_A, OUTPUT_B, OUTPUT> getMergeFunction() {
        return Objects.isNull(mergeFunction) ? (BiFunction<OUTPUT_A, OUTPUT_B, OUTPUT>)DagUtil.getMergeFunction() : mergeFunction;
    }

    class FutureContext {
        AtomicInteger threadCount = new AtomicInteger();
    }

}
