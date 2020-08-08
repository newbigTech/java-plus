package org.java.plus.dag.core.base.proc;

import java.io.Serializable;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

//import com.alibaba.common.lang.diagnostic.Profiler;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
import org.java.plus.dag.core.base.annotation.ConfigInit;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.constants.TppCounterNames;
import org.java.plus.dag.core.base.em.AlgInfoKey;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.exception.StatusType;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.mvel2.MVEL;
import org.java.plus.dag.core.base.utils.CommonMethods;
import org.java.plus.dag.core.base.utils.ContextUtil;
import org.java.plus.dag.core.base.utils.DataParseUtils;
import org.java.plus.dag.core.base.utils.Debugger;
import org.java.plus.dag.core.base.utils.EnumUtil;
import org.java.plus.dag.core.base.utils.HistoryDebugger;
import org.java.plus.dag.core.base.utils.LayerUtils;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.base.utils.RealDebugger;
import org.java.plus.dag.core.base.utils.ThreadLocalUtils;
import org.java.plus.dag.core.base.utils.TppObjectFactory;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Processor basic framework function
 *
 * @author seven.wxy
 * @date 2018/9/25
 */
public abstract class AbstractProcessor extends AbstractInit implements Processor {
    @Getter
    @Setter
    @ConfigInit(desc = "main processor config key")
    protected String mainChainKey = ConstantsFrame.MAIN_CHAIN_KEY;
    @Getter
    @Setter
    @ConfigInit(desc = "processor execute timeout ms")
    protected int processorTimeout = ConstantsFrame.PROCESSOR_DEFAULT_TIMEOUT_MS;
    @Getter
    @Setter
    @ConfigInit(desc = "call or get other processor result config key")
    protected String processorConfigKey = StringUtils.EMPTY;
    @Getter
    @Setter
    @ConfigInit(desc = "call or get other processor result config json value")
    protected String processorConfigValue = StringUtils.EMPTY;
    @Getter
    @Setter
    @ConfigInit(desc = "async execute processor")
    protected boolean async = false;

    @ConfigInit(desc = "need distinct or not")
    protected boolean needDistinct = false;

    @ConfigInit(desc = "need sort or not")
    protected boolean needSort = false;
    @ConfigInit(desc = "sort field")
    protected String sortField = "score";

    @ConfigInit(desc = "need cutoff or not")
    protected boolean needCutoff = false;
    @ConfigInit(desc = "cutoff count, -1 return processorContext.getCount()")
    protected Integer cutoffCount = -1;
    @ConfigInit(desc = "cutoff count param")
    protected String cutoffParam = StringUtils.EMPTY;
    @ConfigInit(desc = "random cutoff or not")
    protected boolean randomCutoff = false;

    @ConfigInit(desc = "only retain current processor result after processor execute or not")
    protected boolean onlyRetainCurrentProcessorResult = false;
    @ConfigInit(desc = "use empty otherDataSetMap start process or not")
    protected boolean emptyMapStart = false;
    @ConfigInit(desc = "use empty main DataSet start process or not")
    protected boolean emptyDataSetStart = false;
    @ConfigInit(desc = "trigger getData action or not")
    protected boolean action = false;

    @ConfigInit(desc = "default key-value per row, json config")
    protected String keyValuePerRow = StringUtils.EMPTY;
    protected Map<AllFieldName, Object> keyValuePerRowMap;
    @ConfigInit(desc = "default algInfo key-value per row, json config")
    protected String algInfoPerRow = StringUtils.EMPTY;
    protected Map<AlgInfoKey, String> algInfoPerRowMap;
    @ConfigInit(desc = "default extData key-value per row, json config")
    protected String extDataPerRow = StringUtils.EMPTY;
    protected Map<String, Object> extDataPerRowMap;
    @ConfigInit(desc = "algInfo from row key-value, json config")
    protected String algInfoFromKeyValue = StringUtils.EMPTY;
    protected Map<AlgInfoKey, AllFieldName> algInfoFromKeyValueMap;

    @ConfigInit(desc = "extData from row key-value, json config")
    protected String extDataFromKeyValue = StringUtils.EMPTY;
    protected Map<AllFieldName, String> extDataFromKeyValueMap;

    @ConfigInit(desc = "DataSet first row data put to context key mapping, key:Row key,value:Context key, json config")
    protected String firstRowValueToContextKeyMapping = StringUtils.EMPTY;
    protected Map<String, String> firstRowValueToContextKeyMap;
    @ConfigInit(desc = "DataSet multi row data put to context key mapping, multi value is join on ',', key:Row key,value:Context key, json config")
    protected String multiRowValueToContextKeyMapping = StringUtils.EMPTY;
    protected Map<String, String> multiRowValueToContextKeyMap;
    @ConfigInit(desc = "DataSet multi value connector")
    protected String multiRowValueConnector = StringPool.COMMA;

    @ConfigInit(desc = "mock data, json list config")
    protected String mockData = StringUtils.EMPTY;
    protected List<Row> mockDataList;

    @ConfigInit(desc = "algInfo keys in contextData")
    protected String contextAlgInfoKeys = StringUtils.EMPTY;
    @ConfigInit(desc = "algInfo keys in contextData prefix")
    protected String contextAlgInfoKeysPrefix = ConstantsFrame.INSERT_CONTEXT_KEY_ALG_INFO;

    @ConfigInit(desc = "extData keys in contextData")
    protected String contextExtDataKeys = StringUtils.EMPTY;

    @ConfigInit(desc = "enable current processor or not")
    @Getter
    protected boolean enableProcessor = true;

    @ConfigInit(desc = "score + or -")
    protected Double scoreAdd = 0D;

    @ConfigInit(desc = "processor empty result counter")
    protected boolean emptyResultCounter = false;

    @ConfigInit(desc = "skip when alreadyCost - expectMs > 0, default 0 is disable skip")
    protected long expectMs = 0;

    @ConfigInit(desc = "enable processor manual or not")
    private boolean enableManual = false;

    @ConfigInit(desc = "processor execute condition, if condition is not satisfied, processor will use it't input as output. Empty str means no condition")
    private String condition = StringUtils.EMPTY;
    private Serializable compiledCondition;

    @ConfigInit(desc = "final result extData config")
    protected String resultExtData = StringUtils.EMPTY;
    protected Map<String, Object> resultExtDataMap;

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    @Override
    public void init(ProcessorConfig processorConfig) {
        super.init(processorConfig);
        this.initConfigMap();
        if (enableManual && !Debugger.isLocal()) {
            LayerUtils.writeManualToTair(allFieldsList, this);
        }
        allFieldsList.clear();
        initProcessorCondition();
    }

    private void initProcessorCondition() {
        if (StringUtils.isNotEmpty(condition)) {
            compiledCondition = MVEL.compileExpression(condition);
        }
    }

    /**
     * do process function, each request is executed
     *
     * @param context     processor context
     * @param mainDataSet the main thread result DataSet from the processor start to end
     * @param dataSetMap  the other current thread processor result DataSet,Map key=Processor.instanceKey,Values=DataSet
     * @return DataSet
     */
    public abstract DataSet<Row> doBaseProcess(ProcessorContext context,
                                               DataSet<Row> mainDataSet,
                                               Map<String, DataSet<Row>> dataSetMap);


    private void initConfigMap() {
        keyValuePerRowMap = parseConfigMap(keyValuePerRow, "Key-Value", new TypeReference<Map<AllFieldName, Object>>() {});
        keyValuePerRowMap = parseKeyValuePerRowMap(keyValuePerRowMap);
        algInfoPerRowMap = parseConfigMap(algInfoPerRow, "AlgInfo Key-Value", new TypeReference<Map<AlgInfoKey, String>>() {});
        algInfoFromKeyValueMap = parseConfigMap(algInfoFromKeyValue, "AlgInfo from Key-Value", new TypeReference<Map<AlgInfoKey, AllFieldName>>() {});
        extDataPerRowMap = parseConfigMap(extDataPerRow, "ExtData Key-Value", new TypeReference<Map<String, Object>>() {});
        extDataFromKeyValueMap = parseConfigMap(extDataFromKeyValue, "ExtData from Key-Value", new TypeReference<Map<AllFieldName, String>>() {});
        firstRowValueToContextKeyMap = parseConfigMap(firstRowValueToContextKeyMapping, "First row value to context key", new TypeReference<Map<String, String>>() {});
        multiRowValueToContextKeyMap = parseConfigMap(multiRowValueToContextKeyMapping, "Multi row value to context key", new TypeReference<Map<String, String>>() {});
        mockDataList = parseConfigList(mockData, "Mock data", new TypeReference<List<Row>>() {});
        resultExtDataMap = parseConfigMap(resultExtData, "final ExtData Key-Value", new TypeReference<Map<String, Object>>() {});
    }

    protected  <T> T parseConfigMap(String configValue, String errorMessage, TypeReference<T> typeReference) {
        return parseConfigMap(configValue, errorMessage, typeReference, null);
    }

    protected  <T> T parseConfigMap(String configValue, String errorMessage, TypeReference<T> typeReference, T defaultResult) {
        T resultMap = defaultResult;
        try {
            if (StringUtils.isNotEmpty(configValue)) {
                resultMap = JSONObject.parseObject(configValue, typeReference);
            }
        } catch (Exception e) {
            Logger.onlineWarn(() -> errorMessage + " map parse error, " + configValue);
        }
        return resultMap;
    }

    private <T> T parseConfigList(String configValue, String errorMessage, TypeReference<T> typeReference) {
        T resultList = null;
        try {
            if (StringUtils.isNotEmpty(configValue)) {
                resultList = JSONArray.parseObject(configValue, typeReference);
            }
        } catch (Exception e) {
            Logger.onlineWarn(() -> errorMessage + " list parse error, " + configValue);
        }
        return resultList;
    }

    private Map<AllFieldName, Object> parseKeyValuePerRowMap(Map<AllFieldName, Object> keyValuePerRowMap) {
        try {
            if (MapUtils.isNotEmpty(keyValuePerRowMap)) {
                Iterator<Map.Entry<AllFieldName, Object>> it = keyValuePerRowMap.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<AllFieldName, Object> entry = it.next();
                    AllFieldName allFieldName = entry.getKey();
                    Object value = entry.getValue();
                    if (allFieldName.getClazz().isEnum()) {
                        if (Objects.nonNull(value)) {
                            value = EnumUtils.getEnum((Class) allFieldName.getClazz(), value.toString());
                            if (Objects.isNull(value)) {
                                it.remove();
                            } else {
                                keyValuePerRowMap.put(allFieldName, value);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            Logger.onlineWarn(() -> "Key-Value map parse error, " + keyValuePerRowMap);
        }
        return keyValuePerRowMap;
    }

    private boolean conditionSatisfied(ProcessorContext context, DataSet<Row> mainDataSet,
                                       Map<String, DataSet<Row>> dataSetMap) {
        if (null == compiledCondition) {
            return true;
        }
        Map<String, Object> condVariables = Maps.newHashMap();
        condVariables.put("params", context);
        condVariables.put("dataSet", dataSetMap);
        condVariables.put("mainDataSet", mainDataSet);
        condVariables.put("thisObj", this);
        try {
            Object result = MVEL.executeExpression(compiledCondition, condVariables);
            if (result instanceof Boolean) {
                return (Boolean) result;
            }
            return BooleanUtils.toBoolean(result.toString());
        } catch (Exception e) {
            Logger.warn(() -> "execute mvel failed, " + e.toString());
            return false;
        }
    }

    @Override
    public Map<String, DataSet<Row>> process(ProcessorContext context, Map<String, DataSet<Row>> dataSetMap) {
        long start = System.currentTimeMillis();
        dataSetMap = emptyDataSetStart ? clearMainChainResult(dataSetMap) : dataSetMap;
        DataSet<Row> inputDataSet = getMainChainResult(dataSetMap);
//        Profiler.Entry profileEntry = null;
        Exception exception = null;
        try {
//            processProfiler(context, profileEntry);
            postProcessContext(context);
            preProcessContext(context);
//            profileEntry = getProfilerEntry();
            long alreadyCost;
            if (expectMs != 0 && !CommonMethods.disableTimeout() && (alreadyCost = CommonMethods.alreadyCost(context)) > expectMs) {
                Debugger.put(this, () -> "skip", () -> String.format("expectMs=%s,alreadyCost=%s", expectMs, alreadyCost));
                return dataSetMap;
            }
            if (enableProcessor && conditionSatisfied(context, inputDataSet, dataSetMap)) {
                Map<String, DataSet<Row>> input = emptyMapStart ? Maps.newHashMap() : dataSetMap;
                DataSet<Row> resultDataSet = doBaseProcess(context, inputDataSet, input);
                resultDataSet = pluginProcess(context, resultDataSet);
                writeEmptyResultCounter(resultDataSet);
                dataSetMap = onlyRetainCurrentProcessorResult ? Maps.newHashMap() : dataSetMap;
                dataSetMap.put(this.getInstanceKey(), resultDataSet);
                dataSetMap.put(this.getMainChainKey(), resultDataSet);
            }
        } catch (Exception e) {
            exception = e;
//            ServiceFactory.getTPPCounter().countSum(CommonMethods.getCounterKey(TppCounterNames.PROC_EXCEPTION_COUNTER.getCounterName(), this.getInstanceKey()), 1);
            Logger.error(() -> String.format("ERROR key:%s, msg:%s", this.getInstanceKey(), e.getMessage()), e);
            Debugger.exception(this, StatusType.DOPROCESS_EXCEPTION, e);
        } finally {
            dataSetMap.putIfAbsent(this.getInstanceKey(), inputDataSet);
            dataSetMap.putIfAbsent(this.getMainChainKey(), inputDataSet);
            long cost = System.currentTimeMillis() - start;
            writeDebugInfo(context, cost, dataSetMap, inputDataSet, exception);
            writeCostToTT(context, cost, ConstantsFrame.TYPE_PROCESSOR, getInstanceKey());
            writeTimeoutCounter(context, cost);
            // release the profiler entry
//            processProfiler(context, profileEntry);
            postProcessContext(context);
            preProcessContext(context);
        }
        return dataSetMap;
    }

    protected DataSet<Row> pluginProcess(ProcessorContext context, DataSet<Row> resultDataSet) {
        resultDataSet = needDistinct ? distinctProcess(context, resultDataSet) : resultDataSet;
        resultDataSet = scoreAdd != 0D ? scoreProcess(context, resultDataSet) : resultDataSet;
        resultDataSet = needSort ? sortProcess(context, resultDataSet) : resultDataSet;
        resultDataSet = needCutoff ? cutoffProcess(context, resultDataSet) : resultDataSet;
        resultDataSet = action ? DataSet.toDS(resultDataSet.getData()) : resultDataSet;
        resultDataSet = defaultDataValueProcess(context, resultDataSet);
        resultDataSet = dataToContextProcess(context, resultDataSet);
        resultDataSet = defaultExtDataProcess(context, resultDataSet);
        resultDataSet = algInfoProcess(context, resultDataSet);
        resultDataSet = mockDataToResult(context, resultDataSet);
        return resultDataSet;
    }

    public DataSet<Row> defaultDataValueProcess(ProcessorContext context, DataSet<Row> dataSet) {
        DataSet<Row> result = dataSet;
        try {
            if (MapUtils.isNotEmpty(keyValuePerRowMap)) {
                result = dataSet.fillDefaultValue(keyValuePerRowMap);
            }
        } catch (Exception e) {
            Logger.error("Default data value set error", e);
        }
        return result;
    }

    public DataSet<Row> defaultExtDataProcess(ProcessorContext context, DataSet<Row> dataSet) {
        DataSet<Row> result = dataSet;
        try {
            if (MapUtils.isNotEmpty(extDataPerRowMap)) {
                result = dataSet.fillExtData(extDataPerRowMap);
            }
            if (MapUtils.isNotEmpty(extDataFromKeyValueMap)) {
                extDataFromKeyValueMap.forEach((key, value) ->
                    dataSet.forEach(e -> e.getExtData().put(value, e.getFieldValue(key))));
            }
            if (StringUtils.isNotEmpty(contextExtDataKeys) && Objects.nonNull(result)) {
                Iterable<String> keys = CommonMethods.COMMA_SPLITTER.split(contextExtDataKeys);
                Map<String, Object> map = Maps.newHashMap();
                for (String key : keys) {
                    Object contextValue = context.getContextData(key);
                    if (contextValue != null) {
                        map.put(key, contextValue);
                    }
                }
                if (MapUtils.isNotEmpty(map)) {
                    result = result.fillExtData(map);
                }
            }
            if (MapUtils.isNotEmpty(resultExtDataMap)) {
                context.getExtData().putAll(resultExtDataMap);
            }
        } catch (Exception e) {
            Logger.error("Default extData set error", e);
        }
        return result;
    }

    public DataSet<Row> algInfoProcess(ProcessorContext context, DataSet<Row> dataSet) {
        DataSet<Row> result = dataSet;
        try {
            if (StringUtils.isNotEmpty(contextAlgInfoKeys) && Objects.nonNull(result)) {
                Iterable<String> keys = CommonMethods.COMMA_SPLITTER.split(contextAlgInfoKeys);
                for (String key : keys) {
                    String k = contextAlgInfoKeysPrefix + key;
                    Object contextValue = context.getContextData(k);
                    if (contextValue != null) {
                        result = result.appendAlgInfo(AlgInfoKey.valueOf(key), contextValue);
                    }
                }
            }
            if (MapUtils.isNotEmpty(algInfoPerRowMap)) {
                result = result.appendAlgInfoMap(algInfoPerRowMap);
            }
            if (MapUtils.isNotEmpty(algInfoFromKeyValueMap)) {
                result = result.appendAlgInfoFromKeyValue(algInfoFromKeyValueMap);
            }
        } catch (Exception e) {
            Logger.error("AlgInfo append error", e);
        }
        return result;
    }

    public DataSet<Row> dataToContextProcess(ProcessorContext context, DataSet<Row> dataSet) {
        try {
            if (MapUtils.isNotEmpty(firstRowValueToContextKeyMap)) {
                for (Map.Entry<String, String> entry : firstRowValueToContextKeyMap.entrySet()) {
                    String rowKey = entry.getKey();
                    String contextKey = entry.getValue();
                    if (StringUtils.isNotEmpty(rowKey) && StringUtils.isNotEmpty(contextKey)) {
                        context.addContextData(contextKey, dataSet.getFirstItem()
                            .orElseGet(Row::new)
                            .getFieldValue(EnumUtil.getEnum(rowKey)));
                    }
                }
            }
            if (MapUtils.isNotEmpty(multiRowValueToContextKeyMap)) {
                for (Map.Entry<String, String> entry : multiRowValueToContextKeyMap.entrySet()) {
                    String rowKey = entry.getKey();
                    String contextKey = entry.getValue();
                    if (StringUtils.isNotEmpty(rowKey) && StringUtils.isNotEmpty(contextKey)) {
                        List values = dataSet.getData().stream()
                            .map(row -> row.getFieldValue(EnumUtil.getEnum(rowKey)))
                            .filter(v -> Objects.nonNull(v))
                            .collect(Collectors.toList());
                        context.addContextData(contextKey, Joiner.on(multiRowValueConnector).join(values));
                    }
                }
            }
        } catch (Exception e) {
            Logger.error("Default data value set error", e);
        }
        return dataSet;
    }

    public DataSet<Row> mockDataToResult(ProcessorContext context, DataSet<Row> dataSet) {
        try {
            if (CollectionUtils.isNotEmpty(mockDataList)) {
                List<Row> data = Lists.newArrayList();
                mockDataList.forEach(row -> data.add(new Row(row)));
                dataSet = dataSet.unionAll(new DataSet<>(data));
            }
        } catch (Exception e) {
            Logger.error("Mock data error", e);
        }
        return dataSet;
    }

    /**
     * distinct process
     *
     * @param context
     * @param dataSet
     * @return
     */
    public DataSet<Row> distinctProcess(ProcessorContext context, DataSet<Row> dataSet) {
        return dataSet.distinct(row -> row.getId() + row.getType());
    }

    public DataSet<Row> scoreProcess(ProcessorContext context, DataSet<Row> dataSet) {
        DataSet<Row> result = dataSet;
        try {
            if (Objects.nonNull(scoreAdd) && scoreAdd != 0D) {
                result = DataSet.toDS(dataSet.getData()
                    .stream()
                    .map(row -> row.setScore(row.getFieldValue(AllFieldName.score, 0D) + scoreAdd))
                    .collect(Collectors.toList()));
            }
        } catch (Exception e) {
            Logger.error("Score value add error", e);
        }
        return result;
    }

    /**
     * sort process
     *
     * @param context
     * @param dataSet
     * @return
     */
    public DataSet<Row> sortProcess(ProcessorContext context, DataSet<Row> dataSet) {
        if (StringUtils.equals(AllFieldName.score.name(), sortField)) {
            return dataSet.sort();
        } else {
            return dataSet.sort((left, right) -> ObjectUtils.compare(
                right.getFieldValue(EnumUtil.getEnum(sortField)),
                left.getFieldValue(EnumUtil.getEnum(sortField))));
        }
    }

    /**
     * cutoff process
     *
     * @param context
     * @param dataSet
     * @return
     */
    public DataSet<Row> cutoffProcess(ProcessorContext context, DataSet<Row> dataSet) {
        Integer limit = cutoffCount;
        Integer count = (StringUtils.isNotEmpty(cutoffParam)
            ? ContextUtil.getIntOrDefault(context.getTppContext(), cutoffParam, context.getCount())
            : context.getCount());
        limit = (cutoffCount == -1) ? count : limit;
        dataSet = randomCutoff ? dataSet.shuffle() : dataSet;
        return dataSet.limit(limit);
    }

//    public void processProfiler(ProcessorContext context, Profiler.Entry currentEntry) {
//        if (Debugger.isEnableProfiler() || Debugger.isLocal()) {
//            // release current profiler entry
//            ThreadLocalUtils.releaseEntry(currentEntry);
//            // append current entry to first entry(tpp solution execute entry)
//            ThreadLocalUtils.appendToMainEntry(context.getProfilerStartEntry(), currentEntry);
//        }
//    }

//    private Profiler.Entry getProfilerEntry() {
//        Profiler.Entry result = null;
//        if (Debugger.isEnableProfiler() || Debugger.isLocal()) {
//            // release and reset tpp SolutionExecuteProxy profiler at first time
//            Profiler.release();
//            Profiler.reset();
//            // start new processor profiler
//            Profiler.start(String.format("ServiceName(%s),methodName(%s): ", this.getInstanceKey(), "process"));
//            // save the current entry when async thread execute
//            result = ThreadLocalUtils.getCurrentEntry();
//        }
//        return result;
//    }

    /**
     * init Logger, Debugger and other thread local params
     *
     * @param context
     */
    private void preProcessContext(ProcessorContext context) {
        ThreadLocalUtils.initAllThreadLocal(context);
    }

    /**
     * clear thread local params
     *
     * @param context
     */
    private void postProcessContext(ProcessorContext context) {
        try {
            if (context.getDebug()) {
                writeDebugException(context);
                List<String> logDetail = Logger.getLogDetail();
                if (CollectionUtils.isNotEmpty(logDetail)) {
                    Debugger.put(this, "logDetail", logDetail);
                }
                for (Entry<String, Object> entry : Debugger.getDebugDataMap().entrySet()) {
                    String key = entry.getKey();
                    String thread;
                    if (Debugger.viewThread()) {
                        thread = Thread.currentThread().getId() + StringPool.UNDERSCORE + Thread.currentThread()
                            .getName() + StringPool.UNDERSCORE;
                    } else {
                        thread = Thread.currentThread().getId() + StringPool.UNDERSCORE;
                    }
                    if (Debugger.viewTime()) {
                        thread = thread + CommonMethods.alreadyCost(context) + StringPool.UNDERSCORE;
                    }
                    context.getDebugInfo().put(thread + key, entry.getValue());
                }
            }
        } finally {
            ThreadLocalUtils.clearAllThreadLocal();
        }
    }

    /**
     * write debug exception message Debugger
     *
     * @param context
     */
    private void writeDebugException(ProcessorContext context) {
        List<String> recExceptionList = Debugger.getExceptionList();
        if (CollectionUtils.isNotEmpty(recExceptionList)) {
            Debugger.put(this, Debugger.EXP_SIZE, recExceptionList.size());
            StringBuilder expSb = new StringBuilder();
            for (int i = 0; i < recExceptionList.size(); i++) {
                String message = recExceptionList.get(i);
                expSb.append(i).append(StringPool.COLON).append(message);
            }
            if (expSb.length() > 0) {
                Debugger.put(this, Debugger.EXP, expSb.toString());
            }
        }
    }

    /**
     * get processor main chain DataSet result
     *
     * @param dataSetMap
     * @return
     */
    public DataSet<Row> getMainChainResult(Map<String, DataSet<Row>> dataSetMap) {
        dataSetMap = defaultIfNull(dataSetMap, Maps::newHashMap);
        return dataSetMap.computeIfAbsent(getMainChainKey(), (key) -> new DataSet<>());
    }

    public Map<String, DataSet<Row>> clearMainChainResult(Map<String, DataSet<Row>> dataSetMap) {
        dataSetMap = defaultIfNull(dataSetMap, Maps::newHashMap);
        dataSetMap.put(getMainChainKey(), new DataSet<>());
        return dataSetMap;
    }

    /**
     * support use config key to config json value
     *
     * @param configKey
     * @param configValue
     * @param clazz
     * @param <T>
     * @return
     */
    @SuppressWarnings("Duplicates")
    public <T> T getProcessorByKeyAndConfigValue(String configKey, String configValue, Class<T> clazz) {
        T result;
        JSONObject json = null;
        try {
            json = JSONObject.parseObject(configValue);
        } catch (Exception e) {
            //ignore
        }
        if (Objects.nonNull(json)) {
            result = TppObjectFactory.getBean(configKey, instanceKeyPrefix, json, clazz);
        } else {
            result = TppObjectFactory.getBean(configKey, instanceKeyPrefix, clazz);
        }
        return result;
    }

    public boolean writeCostToTT(ProcessorContext context, long cost, String type, String name) {
        boolean result = false;
        if (context.isWriteProcessorRtToTT()) {
            String content = CommonMethods.getRtTTRecord(type, name, cost);
            result = CommonMethods.writeWithDiscard(ConstantsFrame.RT_TT_TOPIC, ConstantsFrame.RT_TT_ACCESS_KEY, content);
        }
        return result;
    }

    protected boolean isNullOrEmpty(DataSet<Row> dataSet) {
        return Objects.isNull(dataSet) || dataSet.isEmpty();
    }

    protected <T> T defaultIfNull(final T object, Supplier<T> defaultSupplier) {
        return object != null ? object : defaultSupplier.get();
    }

    private void writeEmptyResultCounter(DataSet<Row> resultDataSet) {
        if (emptyResultCounter && isNullOrEmpty(resultDataSet)) {
//            ServiceFactory.getTPPCounter().countSum(
//                CommonMethods.getCounterKey(TppCounterNames.PROC_EMPTY_RESULT.getCounterName(), this.getInstanceKey()), 1);
        }
    }

    private void writeDebugInfo(ProcessorContext context, long cost, Map<String, DataSet<Row>> dataSetMap, DataSet<Row> inputDataSet, Exception exception) {
        DataSet<Row> result = null;
        if (Debugger.isDebug()) {
            result = getMainChainResult(dataSetMap);
            Debugger.put(this, Debugger.SIZE, result.getData().size());
            Debugger.put(this, Debugger.END, DataParseUtils.parseDebugDataSetToBaseDto(result));
            Debugger.put(this, Debugger.COST, cost);
            Debugger.put(this, Debugger.CFG, processorConfig);
            if (context.isRichDebug()) {
                DataSet<Row> finalResult = result;
                CommonMethods.writeDebugDataToTT(this, context,
                    () -> RealDebugger.inputDataSetDebug(this, inputDataSet),
                    () -> finalResult, exception, cost, ConstantsFrame.ONLINE_DEBUG_TT_TOPIC, ConstantsFrame.ONLINE_DEBUG_TT_ACCESS_KEY, true);
            }
        }
        if (!(Debugger.isDebug() && context.isRichDebug()) && HistoryDebugger.isSampleWriteToTunel(context)) {
            DataSet<Row> finalResult = result;
            CommonMethods.writeDebugDataToTT(this, context,
                () -> RealDebugger.inputDataSetDebug(this, inputDataSet),
                () -> finalResult == null ? getMainChainResult(dataSetMap)
                    : finalResult, exception, cost, ConstantsFrame.OFFLINE_DEBUG_TT_TOPIC, ConstantsFrame.OFFLINE_DEBUG_TT_ACCESS_KEY, false);
        }
    }

    private void writeTimeoutCounter(ProcessorContext context, long cost) {
        if (context.isEnableTimeoutCounter() && cost > processorTimeout) {
//            ServiceFactory.getTPPCounter().countSum(
//                CommonMethods.getCounterKey(TppCounterNames.PROC_TIMEOUT.getCounterName(), getInstanceKey()) + "_" + processorTimeout, 1);
        }
    }

    /**
     * this public method is using in mvel
     */
    public boolean curTimeInRange(String startTimeStr, String endTimeStr) {
        LocalTime now = LocalTime.now();
        try {
            LocalTime startTime = LocalTime.parse(startTimeStr, TIME_FORMATTER);
            LocalTime endTime = LocalTime.parse(endTimeStr, TIME_FORMATTER);
            return now.isAfter(startTime) && now.isBefore(endTime);
        } catch (Exception e) {
            return false;
        }
    }
}
