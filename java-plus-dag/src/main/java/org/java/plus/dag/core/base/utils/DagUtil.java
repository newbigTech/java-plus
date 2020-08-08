package org.java.plus.dag.core.base.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

import com.alibaba.fastjson.JSONObject;

import com.google.common.collect.Maps;
//import com.taobao.recommendplatform.protocol.concurrent.SolutionInvoker;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.exception.StatusType;
import org.java.plus.dag.core.base.model.ConfigKey;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.engine.DagEngine;
import org.java.plus.dag.core.engine.DagFunction;
import org.java.plus.dag.core.engine.DagType;
import org.java.plus.dag.core.engine.future.DagEngineFuture;
import org.java.plus.dag.core.engine.queue.DagEngineQueue;
import org.java.plus.dag.core.base.proc.AbstractProcessor;
import org.java.plus.dag.core.base.proc.Processor;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author seven.wxy
 * @date 2019/2/13
 */
public class DagUtil {
    public static DagFunction<ProcessorContext, Map<String, DataSet<Row>>, Map<String, DataSet<Row>>> getDagFunction(Processor processor) {
        if (StringUtils.equals(SolutionConfig.INVOKER, ThreadLocalUtils.getExecuteSolution())) {
            return getDagFunctionExeByInvoker(processor);
        } else {
            return getDagFunctionExeByCurThread(processor);
        }
    }

    public static DagFunction<ProcessorContext, Map<String, DataSet<Row>>, Map<String, DataSet<Row>>> getDagFunctionExeByCurThread(Processor processor) {
        return new DagFunction<ProcessorContext, Map<String, DataSet<Row>>, Map<String, DataSet<Row>>>() {
            @Override
            public String getFunctionName() {
                return processor.getInstanceKey();
            }

            @Override
            public int getTimeoutMs() {
                return processor.getProcessorTimeout();
            }

            @Override
            public Map<String, DataSet<Row>> apply(ProcessorContext context, Map<String, DataSet<Row>> input, Throwable exception) {
                ThreadLocalUtils.initAllThreadLocal(context);
                input = Objects.isNull(input) ? Maps.newHashMap() : input;
                Map<String, DataSet<Row>> result = null;
                String instanceKey = processor.getInstanceKey();
                try {
                    if (Objects.nonNull(exception)) {
                        Debugger.exception(DagUtil.class, StatusType.DOPROCESS_EXCEPTION.getStatus(), "Pre other exception", exception);
                    }
                    final Processor finalProcessor = updateProcessor(processor);
                    instanceKey = finalProcessor.getInstanceKey();
                    Map<String, DataSet<Row>> dependedResult = Maps.newHashMap(input);
                    result = finalProcessor.process(context, dependedResult);
                } catch (Exception e) {
                    Debugger.exception(DagUtil.class, StatusType.DOPROCESS_EXCEPTION.getStatus(), "Other exception", e);
                }
                Map<String, DataSet<Row>> ret =  Objects.isNull(result) ? input : result;
                ret.putIfAbsent(instanceKey, input.getOrDefault(((AbstractProcessor)processor).getMainChainKey(), new DataSet<>()));
                return ret;
            }
        };
    }

    /**
     * Invoke method by SolutionInvoker, support processor timeout
     * @param processor
     * @return
     */
    public static DagFunction<ProcessorContext, Map<String, DataSet<Row>>, Map<String, DataSet<Row>>> getDagFunctionExeByInvoker(Processor processor) {
        return new DagFunction<ProcessorContext, Map<String, DataSet<Row>>, Map<String, DataSet<Row>>>() {
            @Override
            public String getFunctionName() {
                return processor.getInstanceKey();
            }

            @Override
            public int getTimeoutMs() {
                return processor.getProcessorTimeout();
            }

            @Override
            public Map<String, DataSet<Row>> apply(ProcessorContext context, Map<String, DataSet<Row>> input, Throwable exception) {
                long start = System.currentTimeMillis();
                ThreadLocalUtils.initAllThreadLocal(context);
                input = Objects.isNull(input) ? Maps.newHashMap() : input;
                Object result = null;
                if (Objects.nonNull(exception)) {
                    Debugger.exception(DagUtil.class, StatusType.DOPROCESS_EXCEPTION.getStatus(), "Pre proc exp.", exception);
                }
                int timeout = processor.getProcessorTimeout();
                String instanceKey = processor.getInstanceKey();
                try {
                    final Processor finalProcessor = updateProcessor(processor);
                    timeout = finalProcessor.getProcessorTimeout();
                    Map<String, DataSet<Row>> dependedResult = Maps.newHashMap(input);
//                    SolutionInvoker solutionInvoker = ServiceFactory.getSolutionInvoker().begin(DagUtil.class.getName());
//                    solutionInvoker = solutionInvoker.invoke(instanceKey, new ProcessorExecutable(finalProcessor, context, dependedResult));
//                    solutionInvoker.end();
//                    result = solutionInvoker.getResult(instanceKey, timeout);
                } catch (Exception e) {
                    long cost = (System.currentTimeMillis() - start);
                    Debugger.put(DagUtil.class, () -> instanceKey + "_timeout", () -> true);
                    String msg = String.format("invoker timeout in [%d] ms cost [%d]", timeout, cost);
                    Debugger.exception(DagUtil.class, StatusType.DOPROCESS_EXCEPTION.getStatus(), "Pre proc " + msg, e);
                }
                Map<String, DataSet<Row>> ret =  Objects.isNull(result) ? input : (Map<String, DataSet<Row>>)result;
                ret.putIfAbsent(instanceKey, input.getOrDefault(((AbstractProcessor)processor).getMainChainKey(), new DataSet<>()));
                if (Debugger.isDebug()) {
                    Debugger.put(DagUtil.class, String.format("%s_timeout", instanceKey),
                        String.format("config=%d,actual=%d", timeout, (System.currentTimeMillis() - start)));
                }
                return ret;
            }
        };
    }

    static class ProcessorExecutable implements Callable<Map<String, DataSet<Row>>> {
        private Processor processor;
        private ProcessorContext context;
        private Map<String, DataSet<Row>> input;

        ProcessorExecutable(Processor processor, ProcessorContext context, Map<String, DataSet<Row>> input) {
            Objects.requireNonNull(processor);
            Objects.requireNonNull(context);
            Objects.requireNonNull(input);
            this.processor = processor;
            this.context = context;
            this.input = input;
        }

        @Override
        public Map<String, DataSet<Row>> call() throws Exception {
            return processor.process(context, input);
        }
    }

    /**
     * To support processor config update in Dag
     * DAGEngineProcessor will cache the processor instance, processor config update in runtime
     * @param curProcessor
     * @return
     */
    public static Processor updateProcessor(Processor curProcessor) {
        ConfigKey configKey = TppObjectFactory.getConfigKey(curProcessor.getInstanceKey());
        JSONObject configValue = TppObjectFactory.getConfigValue(configKey, ConstantsFrame.NULL_JSON_OBJECT);
        JSONObject curProcessorConfig = Objects.isNull(curProcessor.getProcessorConfig()) ? ConstantsFrame.NULL_JSON_OBJECT : curProcessor.getProcessorConfig();
        if (String.valueOf(configValue).hashCode() != String.valueOf(curProcessorConfig).hashCode()) {
            curProcessor = TppObjectFactory.getBean(configKey.getConfigKey(), configKey.getConfigKeyPrefix(), configValue, Processor.class);
        }
        return curProcessor;
    }

    public static BiFunction<Map<String, DataSet<Row>>, Map<String, DataSet<Row>>, Map<String, DataSet<Row>>> getMergeFunction() {
        return (resultA, resultB) -> {
            Map result = new HashMap(resultA.size() + resultB.size());
            if (resultA instanceof Map && MapUtils.isNotEmpty(resultA)) {
                result.putAll(resultA);
            } else if (Objects.nonNull(resultA)){
                result.put(resultA.toString(), resultA);
            }
            if (resultB instanceof Map && MapUtils.isNotEmpty(resultB)) {
                result.putAll(resultB);
            } else if (Objects.nonNull(resultB)){
                result.put(resultB.toString(), resultB);
            }
            return result;
        };
    }

    public static DagEngine addDependence(DagEngine dagEngine, String fromKey, Processor from, String toKey, Processor to) {
        return dagEngine.addDependence(fromKey, getDagFunction(from), toKey, getDagFunction(to));
    }

    public static long getTimeout(long start, long timeout, boolean removeWaitTime) {
        long alreadyCostTimeMs = System.currentTimeMillis() - start;
        long realWaitTimeOutMs = timeout - alreadyCostTimeMs;
        realWaitTimeOutMs = (realWaitTimeOutMs <= 0L) ? 1L : realWaitTimeOutMs;
        long finalTimeout = removeWaitTime ? realWaitTimeOutMs: timeout;
        boolean debugDisableTimeout = CommonMethods.disableTimeout();
        return debugDisableTimeout ? ConstantsFrame.PROCESSOR_DEBUG_TIMEOUT_MS : finalTimeout;
    }

    public static DagEngine getDagEngine(DagType dagType, String currentInstanceKey) {
        return dagType == DagType.QUEUE ? new DagEngineQueue(currentInstanceKey) : new DagEngineFuture(currentInstanceKey);
    }
}
