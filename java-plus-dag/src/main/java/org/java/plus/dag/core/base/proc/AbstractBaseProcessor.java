package org.java.plus.dag.core.base.proc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSONObject;

import com.google.common.collect.Sets;
import org.java.plus.dag.core.base.annotation.ConfigInit;
import org.java.plus.dag.core.base.cache.CacheType;
import org.java.plus.dag.core.base.cache.CacheUtil;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.exception.StatusType;
import org.java.plus.dag.core.base.model.ConfigKey;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.ContextUtil;
import org.java.plus.dag.core.base.utils.Debugger;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.base.utils.TppObjectFactory;
import org.java.plus.dag.core.ds.IGraphDataSourceBase;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * Processor common util function
 *
 * @author seven.wxy
 * @date 2018/10/8
 */
public abstract class AbstractBaseProcessor extends AbstractProcessor {
    @ConfigInit(desc = "need cache or not")
    protected boolean needCache = false;
    @ConfigInit(desc = "cache ttl(ms). default 0, not expire")
    protected Long cacheTtlMs = 0L;
    @ConfigInit(desc = "cache type. HEAP,OFF_HEAP")
    protected String cacheType = CacheType.OFF_HEAP.name();
    @ConfigInit(desc = "cache key prefix from context")
    protected String cacheKeyPrefixFromContext;
    @ConfigInit(desc = "cache need clone Row or not")
    protected boolean cacheClone = false;
    @ConfigInit(desc = "cache clear algInfo or not")
    protected boolean cacheClearAlgInfo = true;
    @ConfigInit(desc = "cache empty data or not")
    protected boolean emptySaveCache = false;

    /**
     * do process function
     *
     * @param context processor context
     * @param dataSet mainDataSet the main thread result DataSet from the processor start to end
     * @return DataSet
     */
    public abstract DataSet<Row> doProcess(ProcessorContext context, DataSet<Row> dataSet);

    public DataSet<Row> doProcess(ProcessorContext context,
                                  DataSet<Row> mainDataSet,
                                  Map<String, DataSet<Row>> dataSetMap) {
        return doProcess(context, mainDataSet);
    }

    /**
     * make processor result cacheable
     *
     * @param context         processor context
     * @param mainDataSet     the main thread result DataSet from the processor start to end
     * @param otherDataSetMap the other current thread processor result DataSet,Map key=Processor.instanceKey,Values=DataSet
     * @return
     */
    @Override
    public DataSet<Row> doBaseProcess(ProcessorContext context, DataSet<Row> mainDataSet,
                                      Map<String, DataSet<Row>> otherDataSetMap) {
        if (needCache) {
            ConfigKey configKey = TppObjectFactory.getConfigKey(this.getInstanceKey(), instanceKeyPrefix);
            JSONObject configValue = TppObjectFactory.getConfigValue(configKey, ConstantsFrame.NULL_JSON_OBJECT);
            String cacheKey = configKey.toString() + configValue.hashCode();
            if (StringUtils.isNotEmpty(cacheKeyPrefixFromContext)) {
                cacheKey += ContextUtil.getStringOrDefault(context.getTppContext(), cacheKeyPrefixFromContext,
                        context.getContextDataOrDefault(cacheKeyPrefixFromContext, StringUtils.EMPTY));
            }
            Debugger.put(this, "cacheKey", cacheKey);
            List<Row> cacheResult = CacheUtil.readFromCache(cacheKey, CacheType.valueOf(cacheType));
            if (Objects.isNull(cacheResult)) {
                Debugger.put(this, "cacheResultIsNull", true);
                DataSet<Row> queryResult = doProcess(context, mainDataSet, otherDataSetMap);
                cacheResult = queryResult.getData();
                if (cacheResult.size() > 0 || (emptySaveCache && cacheResult.size() == 0)) {
                    CacheUtil.putToCache(cacheKey, cacheResult, CacheType.valueOf(cacheType), cacheTtlMs);
                }
            } else {
                Debugger.put(this, "cacheResultIsNull", false);
            }
            if (cacheClearAlgInfo) {
                if (CollectionUtils.isNotEmpty(cacheResult)) {
                    cacheResult.forEach(row -> row.setFieldValue(AllFieldName.algInfo, null));
                }
            }
            if (cacheClone) {
                int size = cacheResult.size();
                if (CollectionUtils.isNotEmpty(cacheResult)) {
                    cacheResult = cacheResult.parallelStream().filter(Objects::nonNull)
                            .map(Row::cloneRow).collect(Collectors.toCollection(() -> new ArrayList<>(size)));
                }
            }
            return new DataSet<>(cacheResult);
        } else {
            return doProcess(context, mainDataSet, otherDataSetMap);
        }
    }

    /**
     * get result by processor config key
     * execute processor when dataSetMap does not contains the processor key
     *
     * @param processorConfigKey
     * @param context
     * @param mainDataSet
     * @param dataSetMap
     * @return
     */
    protected DataSet<Row> getDataSetByProcessorConfigKey(String processorConfigKey,
                                                          ProcessorContext context,
                                                          DataSet<Row> mainDataSet,
                                                          Map<String, DataSet<Row>> dataSetMap) {
        return getDataSetByProcessorConfigKey(processorConfigKey, context, mainDataSet, dataSetMap, true);
    }

    /**
     * get result by processor config key
     *
     * @param processorConfigKey
     * @param context
     * @param mainDataSet
     * @param dataSetMap
     * @param executeProcessorWhenKeyNotExists execute processor or not when dataSetMap does not contains the processor key
     * @return
     */
    protected DataSet<Row> getDataSetByProcessorConfigKey(String processorConfigKey,
                                                          ProcessorContext context,
                                                          DataSet<Row> mainDataSet,
                                                          Map<String, DataSet<Row>> dataSetMap,
                                                          boolean executeProcessorWhenKeyNotExists) {
        if (StringUtils.isEmpty(processorConfigKey)) {
            return new DataSet<>();
        }
        return getDataSetByConfigKeyAndValue(processorConfigKey, StringUtils.EMPTY, context, mainDataSet, dataSetMap, executeProcessorWhenKeyNotExists, false);
    }

    protected DataSet<Row> getDataSetByProcessorConfigKey(String processorConfigKey,
                                                          ProcessorContext context,
                                                          DataSet<Row> mainDataSet,
                                                          Map<String, DataSet<Row>> dataSetMap,
                                                          boolean executeProcessorWhenKeyNotExists,
                                                          boolean useInputMainDataSetExecute) {
        if (StringUtils.isEmpty(processorConfigKey)) {
            return new DataSet<>();
        }
        return getDataSetByConfigKeyAndValue(processorConfigKey, StringUtils.EMPTY, context, mainDataSet, dataSetMap, executeProcessorWhenKeyNotExists, useInputMainDataSetExecute);
    }

    protected DataSet<Row> getDataSetByConfigKeyAndValue(String processorConfigKey,
                                                         String processorConfigValue,
                                                         ProcessorContext context,
                                                         DataSet<Row> mainDataSet,
                                                         Map<String, DataSet<Row>> dataSetMap) {
        return getDataSetByConfigKeyAndValue(processorConfigKey, processorConfigValue, context, mainDataSet, dataSetMap, true, false);
    }

    /**
     * get result by processor config key and config value(json string)
     *
     * @param processorConfigKey               config key
     * @param processorConfigValue             config value(json string)
     * @param context
     * @param mainDataSet
     * @param dataSetMap
     * @param executeProcessorWhenKeyNotExists execute processor or not when dataSetMap does not contains the processor key
     * @return
     */
    protected DataSet<Row> getDataSetByConfigKeyAndValue(String processorConfigKey,
                                                         String processorConfigValue,
                                                         ProcessorContext context,
                                                         DataSet<Row> mainDataSet,
                                                         Map<String, DataSet<Row>> dataSetMap,
                                                         boolean executeProcessorWhenKeyNotExists,
                                                         boolean useInputMainDataSetExecute) {
        DataSet<Row> ret = new DataSet<>();
        if (StringUtils.isEmpty(processorConfigKey)) {
            return ret;
        }
        try {
            String realConfigKey = TppObjectFactory.getConfigKey(processorConfigKey, instanceKeyPrefix).toString();
            if (StringUtils.isNotEmpty(realConfigKey)) {
                if (dataSetMap.containsKey(realConfigKey)) {
                    ret = dataSetMap.get(realConfigKey);
                } else {
                    if (executeProcessorWhenKeyNotExists) {
                        // mainDataSet set to empty DataSet, if use mainDataSet, get value from dataSetMap
                        Map<String, DataSet<Row>> result = callProcessorByConfigKeyAndValue(realConfigKey,
                                processorConfigValue, context, useInputMainDataSetExecute ? mainDataSet : ret, dataSetMap);
                        ret = result.get(realConfigKey);
                        dataSetMap.put(realConfigKey, ret);
                    }
                }
            }
        } catch (Exception e) {
            Logger.onlineWarn(
                    String.format("Call processor %s ,error: %s", this.getInstanceKey(), ExceptionUtils.getStackTrace(e)));
            Debugger.exception(this, StatusType.DOPROCESS_EXCEPTION, e);
        }
        return ret == null ? new DataSet<>() : ret;
    }

    /**
     * get result by "processorConfigKey" config
     * execute processor when dataSetMap does not contains the processor key
     *
     * @param context
     * @param mainDataSet
     * @param dataSetMap
     * @return
     */
    protected DataSet<Row> getDataSetByDefaultProcessorConfigKey(ProcessorContext context,
                                                                 DataSet<Row> mainDataSet,
                                                                 Map<String, DataSet<Row>> dataSetMap) {
        return getDataSetByProcessorConfigKey(processorConfigKey, context, mainDataSet, dataSetMap);
    }

    public IGraphDataSourceBase getIGraphDataSourceByConfigValue(String configValue) {
        return getProcessorByKeyAndConfigValue(ConstantsFrame.IGRAPH_DATA_SOURCE_NAME, configValue, IGraphDataSourceBase.class);
    }

    /**
     * execute processor by config key to get result
     *
     * @param processorConfigKey
     * @param processorConfigValue
     * @param context
     * @param mainDataSet
     * @param dataSetMap
     * @return
     */
    public Map<String, DataSet<Row>> callProcessorByConfigKeyAndValue(String processorConfigKey,
                                                                      String processorConfigValue,
                                                                      ProcessorContext context,
                                                                      DataSet<Row> mainDataSet,
                                                                      Map<String, DataSet<Row>> dataSetMap) {
        if (StringUtils.isEmpty(processorConfigKey)) {
            return dataSetMap;
        }
        Processor processor;
        if (StringUtils.isNotEmpty(processorConfigValue)) {
            processor = getProcessorByKeyAndConfigValue(processorConfigKey, processorConfigValue, Processor.class);
        } else {
            processor = TppObjectFactory.getBean(processorConfigKey, instanceKeyPrefix, Processor.class);
        }
        if (Objects.isNull(processor)) {
            return dataSetMap;
        } else {
            return processor.process(context, dataSetMap);
        }
    }

    /**
     * Parse Set config from String
     *
     * @param stringParam
     * @return
     */
    protected Set<Integer> parseSetParameter(String stringParam) {
        Set<Integer> paramSet = Sets.newHashSet();
        if (StringUtils.isNotEmpty(stringParam)) {
            String[] array = stringParam.split(StringPool.COMMA);
            for (String str : array) {
                paramSet.add(Integer.parseInt(str));
            }
        }
        return paramSet;
    }

}
