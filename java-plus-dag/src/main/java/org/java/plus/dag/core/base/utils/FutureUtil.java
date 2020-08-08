package org.java.plus.dag.core.base.utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.Maps;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.model.TaskExecutor;
import org.java.plus.dag.core.base.proc.Processor;
import org.apache.commons.collections4.MapUtils;

/**
 * @author seven.wxy
 * @date 2018/10/11
 */
public class FutureUtil {
    public static Supplier<Map<String, DataSet<Row>>> executeProcessor(Processor processor,
                                                                       ProcessorContext ctx,
                                                                       Map<String, DataSet<Row>> dataSetMap) {
        return () -> {
            Logger.info(() -> "RUN " + processor.getInstanceKey() + ", THREAD=" + Thread.currentThread().getId());
            Map<String, DataSet<Row>> result = dataSetMap;
            try {
                result = CompletableFuture.supplyAsync(
                    () -> processor.process(ctx, dataSetMap), getExecutor()).get(processor.getProcessorTimeout(), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                Logger.onlineWarn("RUN timeout" + processor.getInstanceKey() + ", THREAD=" + Thread.currentThread().getId());
            }
            return result;
        };
    }

    public static Map<String, DataSet<Row>> mergeResult(Map<String, DataSet<Row>> resultA,
                                                        Map<String, DataSet<Row>> resultB) {
        Map<String, DataSet<Row>> result = Maps.newHashMapWithExpectedSize(resultA.size() + resultB.size());
        if (MapUtils.isNotEmpty(resultA)) {
            result.putAll(resultA);
        }
        if (MapUtils.isNotEmpty(resultB)) {
            result.putAll(resultB);
        }
        Logger.info(() -> "Merge, THREAD=" + Thread.currentThread().getId());
        return result;
    }

    public static CompletableFuture<Map<String, DataSet<Row>>> createTask(Processor processor,
                                                                          ProcessorContext ctx,
                                                                          Map<String, DataSet<Row>> result) {
        return CompletableFuture.supplyAsync(executeProcessor(processor, ctx, result), getExecutor());
    }

    public static CompletableFuture<Map<String, DataSet<Row>>> createParallelTask(
        CompletableFuture<Map<String, DataSet<Row>>> taskA, CompletableFuture<Map<String, DataSet<Row>>> taskB) {
        return taskA.thenCombineAsync(taskB, (resultA, resultB) -> mergeResult(resultA, resultB), getExecutor());
    }

    public static CompletableFuture<Map<String, DataSet<Row>>> createDependenceTask(
        CompletableFuture<Map<String, DataSet<Row>>> firstTask, ProcessorContext ctx, Processor secondProcessor) {
        Function<Map<String, DataSet<Row>>, CompletableFuture<Map<String, DataSet<Row>>>> depFunction
            = s -> CompletableFuture.supplyAsync(executeProcessor(secondProcessor, ctx, s), (runnable) -> runnable.run());
        return firstTask.thenComposeAsync(depFunction, (runnable) -> runnable.run());
    }

    public static CompletableFuture<Map<String, DataSet<Row>>> createDependenceTaskAsync(
        CompletableFuture<Map<String, DataSet<Row>>> firstTask, ProcessorContext ctx, Processor secondProcessor) {
        Function<Map<String, DataSet<Row>>, CompletableFuture<Map<String, DataSet<Row>>>> depFunction
            = s -> CompletableFuture.supplyAsync(executeProcessor(secondProcessor, ctx, s), getExecutor());
        return firstTask.thenComposeAsync(depFunction, getExecutor());
    }

    public static CompletableFuture<Map<String, DataSet<Row>>> createParallelTasksByList(
        Supplier<Executor> supplier,
        boolean async,
        List<CompletableFuture> tasks) {
        return createParallelTasks(supplier, async, tasks.toArray(new CompletableFuture[tasks.size()]));
    }

    public static CompletableFuture<Map<String, DataSet<Row>>> createParallelTasks(
        Supplier<Executor> supplier,
        boolean async,
        CompletableFuture<Map<String, DataSet<Row>>>... tasks) {
        boolean multiThreadExecute = ThreadLocalUtils.isMultiThreadExecute();
        if (tasks.length != 0) {
            if (tasks.length == 1) {
                return tasks[0];
            } else {
                CompletableFuture<Map<String, DataSet<Row>>> task = tasks[0];
                for (int i = 1; i < tasks.length; i++) {
                    if (multiThreadExecute && async) {
                        task = task.thenCombineAsync(tasks[i],
                            (resultA, resultB) -> mergeResult(resultA, resultB), supplier.get());
                    } else {
                        task = task.thenCombine(tasks[i],
                            (resultA, resultB) -> mergeResult(resultA, resultB));
                    }
                }
                return task;
            }
        } else {
            return CompletableFuture.completedFuture(Maps.newHashMap());
        }
    }

    public static Executor getExecutor() {
        boolean multiThreadExecute = ThreadLocalUtils.isMultiThreadExecute();
        boolean useTppThreadPool = ThreadLocalUtils.isUseTppThreadPool();
        boolean useNewWispThreadPool = ThreadLocalUtils.isUseNewWispThreadPool();
        return TaskExecutor.findExecutor(multiThreadExecute, useTppThreadPool, useNewWispThreadPool);
    }
}
