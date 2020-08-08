package org.java.plus.dag.core.engine;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.java.plus.dag.core.base.annotation.ConfigInit;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.exception.StatusType;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.mvel2.MVEL;
import org.java.plus.dag.core.base.proc.BaseProcessor;
import org.java.plus.dag.core.base.proc.Processor;
import org.java.plus.dag.core.base.utils.DagUtil;
import org.java.plus.dag.core.base.utils.Debugger;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.base.utils.TppObjectFactory;
import org.java.plus.dag.core.engine.condition.ConditionConfig;
import org.java.plus.dag.core.engine.condition.ConditionContext;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author seven.wxy
 * @date 2019/1/30
 */
public class DagProcessor extends BaseProcessor {
    protected final static String PROCESSORS_KEY = "processors";
    protected final static String PROCESSOR_DEPT_KEY = "processorDependencies";
    protected final static String CONDITION_KEY = "conditions";
    /**
     * ConfigKey contains "==" is conditionKey
     */
    protected final static String CONDITION_KEY_FLAG = "==";

    protected DagEngine<ProcessorContext> engine;
    protected Map<String, Set<ConditionConfig>> processorConditions = Maps.newHashMap();

    /**
     * DAG data struct init success or not
     */
    protected boolean init = false;

    @ConfigInit(desc = "dag processor and dag engine timeout gap ms")
    protected int timeoutGap = 0;

    @ConfigInit(desc = "dag type: FutureDag,QueueDag")
    protected String dagType = DagType.QUEUE.name();

    @ConfigInit(desc = "thread count per query in dag engine")
    protected int threadCntPerQuery = 0;

    @ConfigInit(desc = "timeout remove real wait time")
    protected boolean removeWaitTime = true;

    @ConfigInit(desc = "clear main chain result before dag execution")
    protected boolean clearMainChainResult = true;

    @ConfigInit(desc = "skip tail processor when timeout or not")
    protected boolean skipTimeout = true;

    @Override
    public void doInit(ProcessorConfig conf) {
        init = initEngine(conf);
        if (!init) {
            Logger.warn(() -> this.getInstanceKey() + " init engine failed");
        }
    }

    @Override
    public DataSet<Row> doProcess(ProcessorContext context, DataSet<Row> mainDataSet, Map<String, DataSet<Row>> dataSetMap) {
        long start = System.currentTimeMillis();
        if (!init) {
            Logger.error(() -> this.getInstanceKey() + " dag not init");
            Debugger.put(this, "exception_not_init", true);
            return new DataSet<>();
        }
        Map<String, DataSet<Row>> result = null;
        Map<String, DataSet<Row>> dagInput = Maps.newHashMap(dataSetMap);
        if (clearMainChainResult) { clearMainChainResult(dagInput); }
        try {
            ProcessorContext cloneContext = context.clone();
            result = engine.run(cloneContext, dagInput);
            ProcessorContext.mergeContextData(context, cloneContext);
        } catch (Exception e) {
            long end = System.currentTimeMillis();
            String msg = "expect timeout:" + getProcessorTimeout() + ",actual cost:" + (end - start);
            Debugger.exception(this, StatusType.DOPROCESS_EXCEPTION.getStatus(), msg, e);
        }
        long end = System.currentTimeMillis();
        if (Debugger.isDebug()) {
            Debugger.put(this, "dag_execute_cost", (end - start));
            Debugger.put(this, "dag_class", engine.getClass().getSimpleName());
        }
        if (MapUtils.isEmpty(result)) {
            return new DataSet<>();
        }
        for (Map.Entry<String, DataSet<Row>> entry : result.entrySet()) {
            if (Objects.nonNull(entry.getValue())) {
                dataSetMap.put(entry.getKey(), entry.getValue());
            }
        }
        return this.getMainChainResult(dataSetMap);
    }

    @SuppressWarnings("unchecked")
    private boolean initEngine(ProcessorConfig conf) {
        engine = DagUtil.getDagEngine(DagType.valueOf(dagType), this.getInstanceKey());
        engine.setThreadCntPerQuery(threadCntPerQuery);
        engine.setTimeout(processorTimeout + timeoutGap);
        engine.setRemoveWaitTime(removeWaitTime);
        engine.setAsync(async);
        engine.setMergeFunction(DagUtil.getMergeFunction());
        engine.setSkipTimeout(skipTimeout);
        if (!conf.containsKey(PROCESSORS_KEY) || !conf.containsKey(PROCESSOR_DEPT_KEY)) {
            Logger.warn(() -> this.getInstanceKey() + " contains none processors key or processor dependencies");
            return false;
        }
        Map<String, Processor> processors = genProcessors(conf);
        if (MapUtils.isEmpty(processors)) {
            Logger.warn(() -> this.getInstanceKey() + " gen processors failed");
            return false;
        }
        Map<String, Serializable> conditions = Maps.newHashMap();
        JSONObject conditionConf = ObjectUtils.defaultIfNull(conf.getJSONObject(CONDITION_KEY), ConstantsFrame.NULL_JSON_OBJECT);
        conditionConf.forEach((key, expression) -> conditions.put(key, MVEL.compileExpression(expression.toString())));
        Debugger.put(this, () -> "conditions", () -> conditions.toString());
        for (Map.Entry<String, Object> entry : conf.getJSONObject(PROCESSOR_DEPT_KEY).entrySet()) {
            if (!addDepend(processors, conditions, entry.getKey(), entry.getValue())) {
                Logger.warn(() -> this.getInstanceKey() + " add dependency failed");
                return false;
            }
        }
        engine.setConditions(initConditions(processors, conditions));

        if (BooleanUtils.isNotTrue(engine.initDagStruct())) {
            Logger.warn(() -> this.getInstanceKey() + " DAGEngine init failed");
            return false;
        }
        return true;
    }

    private void addProcessorCondition(String conditionKey, String expectedVal, String procKey) {
        if (!processorConditions.containsKey(procKey)) {
            processorConditions.put(procKey, Sets.newHashSet());
        }
        processorConditions.get(procKey).add(new ConditionConfig(conditionKey, expectedVal));
    }

    private boolean resolveCondition(Map<String, Serializable> conditions, String fromKey, Object toKeys) {
        String conditionKey;
        String expectedVal;
        try {
            String[] splitVal = StringUtils.split(fromKey, CONDITION_KEY_FLAG);
            conditionKey = splitVal[0];
            expectedVal = splitVal[1];
        } catch (Exception e) {
            Logger.warn(() -> this.getInstanceKey() + " parse condition key failed");
            return false;
        }

        if (!conditions.containsKey(conditionKey)) {
            Logger.warn(() -> this.getInstanceKey() + " parse condition key failed, not contains key:" + conditionKey);
            return false;
        }
        if (toKeys instanceof String) {
            addProcessorCondition(conditionKey, expectedVal, toKeys.toString());
        } else if (toKeys instanceof JSONArray) {
            for (Object oneKey : (JSONArray)toKeys) {
                addProcessorCondition(conditionKey, expectedVal, oneKey.toString());
            }
        }
        return true;
    }

    private boolean addDepend(Map<String, Processor> processors, Map<String, Serializable> conditions,
                              String fromKey, Object toKey) {
        if (isConditionKey(fromKey)) {
            return resolveCondition(conditions, fromKey, toKey);
        }
        if (toKey instanceof String) {
            Processor from = processors.get(fromKey);
            Processor to = processors.get(toKey);
            if (Objects.isNull(from) || Objects.isNull(to)) {
                return false;
            }
            DagUtil.addDependence(engine, fromKey, from, String.valueOf(toKey), to);
            return true;
        }
        if (!(toKey instanceof JSONArray)) {
            Logger.warn(() -> this.getInstanceKey() + " dependency value is not json array");
            return false;
        }
        for (Object oneKey : (JSONArray)toKey) {
            if (!(oneKey instanceof String)) {
                Logger.warn(() -> this.getInstanceKey() + " key is not string");
                return false;
            }
            Processor from = processors.get(fromKey);
            Processor to = processors.get(oneKey);
            if (Objects.isNull(from) || Objects.isNull(to)) {
                return false;
            }
            DagUtil.addDependence(engine, fromKey, from, String.valueOf(oneKey), to);
        }
        return true;
    }

    private boolean isConditionKey(String condition) {
        return StringUtils.contains(condition, CONDITION_KEY_FLAG);
    }

    private Map<String, Predicate<ConditionContext>> initConditions(Map<String, Processor> processors,
                                                                    Map<String, Serializable> conditions) {
        Map<String, Predicate<ConditionContext>> result = Maps.newConcurrentMap();
        if (MapUtils.isEmpty(processorConditions)) {
            return result;
        }
        processorConditions.forEach((procKey, conditionSet) -> {
            if (processors.containsKey(procKey)) {
                result.put(procKey, getPredicate(conditions, conditionSet));
            }
        });
        return result;
    }

    private Predicate<ConditionContext> getPredicate(Map<String, Serializable> conditions,
                                                     Set<ConditionConfig> conditionSet) {
        return (conditionParam) -> {
            Map<String, Object> vars = Maps.newHashMap();
            if (Objects.nonNull(conditionParam.getContext())) {
                vars.put("params", conditionParam.getContext());
            }
            vars.put("dataSet", conditionParam.getDependedResult());
            for (ConditionConfig condition : conditionSet) {
                if (!conditions.containsKey(condition.getConditionKey())) {
                    continue;
                }
                try {
                    String conditionResult = MVEL.executeExpression(conditions.get(condition.getConditionKey()), vars).toString();
                    boolean needRun = conditionResult.equals(condition.getExpectedValue());
                    Debugger.put(this,
                        () -> "condition_result#" + condition.getConditionKey() + "==" + condition.getExpectedValue(),
                        () -> needRun);
                    if (!needRun) {
                        return false;
                    }
                } catch (Exception e) {
                    Logger.warn(() -> " execute mvel failed");
                }
            }
            return true;
        };
    }

    private Map<String, Processor> genProcessors(ProcessorConfig conf) {
        Map<String, Processor> processors = Maps.newHashMap();
        for (Map.Entry<String, Object> entry : conf.getJSONObject(PROCESSORS_KEY).entrySet()) {
            String className = (String)entry.getValue();
            Processor proc = TppObjectFactory.getBean(className, instanceKeyPrefix, Processor.class);
            if (Objects.isNull(proc)) {
                Logger.warn(() -> String.format("Get processor bean failed, class name=%s", className));
            } else {
                processors.put(entry.getKey(), proc);
            }
        }
        return processors;
    }
}
