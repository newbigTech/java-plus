package org.java.plus.dag.core.service.merge;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
import org.java.plus.dag.core.base.annotation.ConfigInit;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.constants.TppCounterNames;
import org.java.plus.dag.core.base.em.AlgInfoKey;
import org.java.plus.dag.core.base.em.AlgInfoKeyDebug;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.ContextUtil;
import org.java.plus.dag.core.base.utils.Debugger;
import org.java.plus.dag.core.base.utils.EnumUtil;
import org.java.plus.dag.core.base.utils.RandomUtils;
import org.java.plus.dag.core.base.utils.ScatterUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * Common scatter framework
 *
 * @author seven.wxy
 * @date 2019/7/8
 */
public class CommonBaseMerge extends BaseMerge {
    protected static final String RATIO_KEY = "ratio";
    protected static final String START_POS_KEY = "startIndex";
    protected static final String DATA_SOURCE_KEY = "dataSourceKey";
    protected static final String FORCE_TOP_KEY = "forceTopN";
    protected static final String BACKUP_CONFIG_KEY = "backupKey";
    protected static final String DATA_SOURCE_INDICATOR = "dataSourceIndicator";
    protected static final String BACKUP_KEY = "backup";
    protected static final String ENABLE_KEY = "enable";
    protected static final String SCATTER_KEY = "scatter";
    protected static final String MOD_PREFIX = "mod";
    protected static final String EMPTY_CONFIG = "{}";
    protected static final Integer DEFAULT_PAGE_CONFIG_NUMBER = 0;
    protected static final Splitter COMMA_SPLITTER = Splitter.on(StringPool.COMMA);
    protected static final Splitter EQUAL_SPLITTER = Splitter.on(StringPool.EQUALS);
    @ConfigInit(desc = "merge ratio/sourceKey... config json, demo:{"
        + "  \"video_recall\": {"
        + "    \"ratio\": 0.7,"
        + "    \"dataSourceKey\": \"\","
        + "    \"backupKey\": \"new_hot_recall,backup_recall\""
        + "  },"
        + "  \"tag_recall\": {"
        + "    \"dataSourceKey\": \"\","
        + "    \"forceTopN\": 5,"
        + "    \"enable\": false,"
        + "    \"scatter\": false"
        + "  },"
        + "  \"new_hot_recall\": {"
        + "    \"ratio\": 0.3,"
        + "    \"startIndex\": \"4\","
        + "    \"dataSourceKey\": \"\""
        + "  },"
        + "  \"backup_recall\": {"
        + "    \"ratio\": 0.2,"
        + "    \"dataSourceKey\": \"\","
        + "    \"backup\": true"
        + "  }"
        + "}")
    protected String mergeConfig = EMPTY_CONFIG;

    @ConfigInit(desc = "merge max type count, value=-1 are not restrict demo:{"
        + "  \"channel\": \"2\","
        + "  \"theme\": \"1\""
        + "}")
    protected String maxTypeCountConfig = EMPTY_CONFIG;

    @ConfigInit(desc = "merge max type count for source data, demo:{"
        + "  \"show\": {"
        + "    \"channel\": \"2\","
        + "    \"theme\": \"1\""
        + "  },"
        + "  \"video\": {"
        + "    \"channel\": \"2\""
        + "  }"
        + "}")
    protected String maxTypeCountConfigForSource = EMPTY_CONFIG;

    @ConfigInit(desc = "force insert item to index, demo:{"
        + "  \"0\": {"
        + "    \"1\": \"new_hot_recall\","
        + "    \"3,4,5\": \"video_recall\","
        + "    \"mod2=0\": \"xxx_recall\""
        + "  },"
        + "  \"1\": {"
        + "    \"1\": \"new_hot_recall\","
        + "    \"3,4,5\": \"video_recall\","
        + "    \"mod2=0\": \"xxx_recall\""
        + "  }"
        + "}")
    protected String forcePosDataConfig = EMPTY_CONFIG;

    @ConfigInit(desc = "force insert item scatter or not")
    protected boolean forceInsertItemNeedScatter = false;

    @ConfigInit(desc = "scatter length")
    protected int scatterLength = 5;

    @ConfigInit(desc = "return result count, -1 is context.getCount")
    protected int returnCount = -1;

    @ConfigInit(desc = "count param name")
    protected String countParam = "count";

    @ConfigInit(desc = "backup write counter or not")
    protected boolean backupWriteCounter = false;

    @ConfigInit(desc = "backup counter key")
    protected String backupCounterKey = TppCounterNames.TPP_BACKUP.getCounterName();

    @ConfigInit(desc = "need parse backup data or not")
    protected boolean needParseBackupData = true;

    @ConfigInit(desc = "need pageWise etc in algInfo")
    protected boolean needPageWise = false;

    @ConfigInit(desc = "enable extra dispatch or not")
    protected boolean enableExtraDispatch = false;

    @ConfigInit(desc = "enable position data extData or not")
    protected boolean enablePosExtData = false;

    @ConfigInit(desc = "enable dynamic ratio or not")
    protected boolean enableDynamicRatio = false;

    @ConfigInit(desc = "enable can't scatter append input data")
    protected boolean cantScatterAppendInput = false;

    @ConfigInit(desc = "enable output scatter flag to algInfo or not")
    protected boolean outputScatterFlag = true;

    @ConfigInit(desc = "use pre-calculated scatter strategy")
    protected boolean usePreCalcScatter = false;
    @ConfigInit(desc = "use random scatter strategy and keep ratio")
    protected boolean useScatterKeepRatio = false;


    protected LinkedHashMap<String, Map<String, String>> mergeConfigMap = Maps.newLinkedHashMap();
    protected LinkedHashMap<String, Integer> maxTypeCountConfigMap = Maps.newLinkedHashMap();
    protected LinkedHashMap<String, LinkedHashMap<String, Integer>> maxTypeCountConfigForSourceMap = Maps.newLinkedHashMap();
    protected LinkedHashMap<Integer, Map<String, String>> forcePosDataConfigMap = Maps.newLinkedHashMap();

    protected Map<String, Integer> forceTopMap;
    protected Map<String, String> dataSourceIndicatorMap;
    protected List<String> backupList;
    /**
     * strategy: {dataType: ratio}
     */
    protected Map<String, Double> ratioMap;
    /**
     * strategy: {dataType: startFromPos}
     */
    protected Map<String, Integer> forceStartFromPosMap;
    /**
     * strategy: {pn : {pos : dataType}}
     */
    protected Map<Integer, Map<Integer, String>> pnPosStrategyMap;
    /**
     * strategy: {pn : {< x%2 , 0> : dataType}}
     */
    protected Map<Integer, Map<ImmutablePair<Integer, Integer>, String>> pnModStrategyMap;
    protected Map<String, List<String>> backupMap;

    protected Map<String, Boolean> scatterMap;

    @Override
    public void doInit(ProcessorConfig processorConfig) {
        mergeConfigMap = JSONObject.parseObject(mergeConfig,
            new TypeReference<LinkedHashMap<String, Map<String, String>>>() {});
        maxTypeCountConfigMap = JSONObject.parseObject(maxTypeCountConfig,
            new TypeReference<LinkedHashMap<String, Integer>>() {});
        maxTypeCountConfigMap = maxTypeCountConfigMap.entrySet().stream().filter(e -> e.getValue() > 0)
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (v1, v2) -> v2, () -> Maps.newLinkedHashMap()));

        maxTypeCountConfigForSourceMap = JSONObject.parseObject(maxTypeCountConfigForSource,
            new TypeReference<LinkedHashMap<String, LinkedHashMap<String, Integer>>>() {});

        forcePosDataConfigMap = JSONObject.parseObject(forcePosDataConfig,
            new TypeReference<LinkedHashMap<Integer, Map<String, String>>>() {});
        forceTopMap = Maps.newLinkedHashMap();
        backupList = Lists.newArrayList();
        ratioMap = Maps.newLinkedHashMap();
        forceStartFromPosMap = Maps.newLinkedHashMap();
        pnPosStrategyMap = Maps.newLinkedHashMap();
        pnModStrategyMap = Maps.newLinkedHashMap();
        backupMap = Maps.newLinkedHashMap();
        dataSourceIndicatorMap = Maps.newLinkedHashMap();
        scatterMap = Maps.newLinkedHashMap();
        mergeConfigMap.entrySet().stream()
            .filter(entry -> Boolean.valueOf(entry.getValue().getOrDefault(ENABLE_KEY, Boolean.TRUE.toString()))
                && StringUtils.isNotEmpty(entry.getValue().get(DATA_SOURCE_KEY)))
            .forEach(entry -> {
                if (entry.getValue().containsKey(RATIO_KEY)) {
                    ratioMap.put(entry.getKey(), Double.valueOf(entry.getValue().get(RATIO_KEY)));
                }
                if (entry.getValue().containsKey(START_POS_KEY)) {
                    forceStartFromPosMap.put(entry.getKey(), Integer.valueOf(entry.getValue().get(START_POS_KEY)));
                }
                if (entry.getValue().containsKey(FORCE_TOP_KEY)) {
                    forceTopMap.put(entry.getKey(), Integer.parseInt(entry.getValue().get(FORCE_TOP_KEY)));
                }
                if (Boolean.valueOf(entry.getValue().get(BACKUP_KEY))) { // for cannot scatter
                    backupList.add(entry.getKey());
                }
                if (entry.getValue().containsKey(BACKUP_CONFIG_KEY)) { // for not enough backup
                    backupMap.put(entry.getKey(), COMMA_SPLITTER.splitToList(entry.getValue().get(BACKUP_CONFIG_KEY)));
                }
                if (entry.getValue().containsKey(DATA_SOURCE_INDICATOR)) {
                    dataSourceIndicatorMap.put(entry.getKey(), entry.getValue().get(DATA_SOURCE_INDICATOR));
                }
                if (entry.getValue().containsKey(SCATTER_KEY)) {
                    scatterMap.put(entry.getKey(), Boolean.parseBoolean(entry.getValue().get(SCATTER_KEY)));
                }
            });
        forcePosDataConfigMap.forEach((pageNum, configMap) -> {
            if (Objects.nonNull(configMap)) {
                configMap.forEach((key, value) -> {
                    Map<Integer, String> indexMap = pnPosStrategyMap.computeIfAbsent(pageNum, e -> Maps.newLinkedHashMap());
                    Map<ImmutablePair<Integer, Integer>, String> modIndexMap = pnModStrategyMap.computeIfAbsent(pageNum, e -> Maps.newLinkedHashMap());
                    if (StringUtils.contains(key, StringPool.COMMA)) {
                        COMMA_SPLITTER.split(key).forEach(e -> indexMap.putIfAbsent(Integer.parseInt(e), value));
                    } else if (StringUtils.startsWith(key, MOD_PREFIX)) {
                        String mod = StringUtils.substring(key, MOD_PREFIX.length());
                        List<String> modList = EQUAL_SPLITTER.splitToList(mod);
                        ImmutablePair<Integer, Integer> pair = ImmutablePair.of(Integer.parseInt(modList.get(0)),
                            Integer.parseInt(modList.get(1)));
                        modIndexMap.putIfAbsent(pair, value);
                    } else {
                        indexMap.putIfAbsent(Integer.parseInt(key), value);
                    }
                });
            }
        });
    }

    @Override
    public DataSet<Row> doProcess(ProcessorContext context, DataSet<Row> mainDataSet, Map<String, DataSet<Row>> dataSetMap) {
        // get dataMap from upStream by mergeConfigMap
        Map<String, List<Row>> dataMap = getDataMap(context, mainDataSet, dataSetMap);

        // dataMap pre process
        Map<String, List<Row>> dataMapResult = parseDataMap(context, dataMap);

        Set<String> useBackupKeys = Sets.newHashSet();

        // if empty, fill the dataMap according to backupKey
        dataMapResult = needParseBackupData ? parseBackupDataMap(context, dataMapResult, useBackupKeys) : dataMapResult;

        // write backup counter
        writeBackupCounter(useBackupKeys);

        // do merge
        List<Row> mergeResult = mergeData(context, dataMapResult);

        return new DataSet<>(mergeResult);
    }

    protected Map<Integer, Map<String, String>> getPosExtData(ProcessorContext context) {
        Map<Integer, Map<String, String>> result;
        if (enablePosExtData) {
            result = context.getContextDataOrDefault(ConstantsFrame.CONTEXT_EXTRA_POS_EXT_DATA, HashMap::new);
        } else {
            result = Maps.newHashMap();
        }
        return result;
    }

    protected int getScatterLength(ProcessorContext context) {
        return scatterLength;
    }

    public Map<String, List<Row>> getDataMap(ProcessorContext context, DataSet<Row> mainDataSet, Map<String, DataSet<Row>> dataSetMap) {
        Map<String, List<Row>> dataMap = Maps.newLinkedHashMap();
        mergeConfigMap.entrySet().stream()
            .filter(entry -> Boolean.valueOf(entry.getValue().getOrDefault(ENABLE_KEY, Boolean.TRUE.toString()))
                && StringUtils.isNotEmpty(entry.getValue().get(DATA_SOURCE_KEY)))
            .forEach(entry -> {
                DataSet<Row> dataSet = getDataSetByProcessorConfigKey(entry.getValue().get(DATA_SOURCE_KEY), context,
                    mainDataSet, dataSetMap, false);
                dataMap.put(entry.getKey(), Lists.newLinkedList(dataSet.getData()));
            });
        if (enableExtraDispatch) {
            Map<String, DataSet<Row>> extraDispatchDataMap = context.getContextData(ConstantsFrame.CONTEXT_EXTRA_DISPATCH_DATA_MAP);
            Optional.ofNullable(extraDispatchDataMap)
                .ifPresent(e -> e.forEach((k, v) -> dataMap.put(k, Lists.newLinkedList(v.getData()))));
        }
        return dataMap;
    }

    /**
     * Data parse, return value list must be LinkedList
     *
     * @param context
     * @param dataMap
     * @return
     */
    public Map<String, List<Row>> parseDataMap(ProcessorContext context, Map<String, List<Row>> dataMap) {
        return dataMap;
    }

    public Map<String, List<Row>> parseBackupDataMap(ProcessorContext context, Map<String, List<Row>> dataMap, Set<String> useBackupKeys) {
        for (Map.Entry<String, List<Row>> entry : dataMap.entrySet()) {
            List<String> backupKeys = backupMap.get(entry.getKey());
            if (CollectionUtils.isNotEmpty(backupKeys) && CollectionUtils.isEmpty(entry.getValue())) {
                for (String backupKey : backupKeys) {
                    List<Row> backupData = dataMap.get(backupKey);
                    if (CollectionUtils.isNotEmpty(backupData)) {
                        dataMap.put(entry.getKey(), Lists.newLinkedList(backupData));
                        useBackupKeys.add(entry.getKey() + StringPool.UNDERSCORE + backupKey);
                        break;
                    }
                }
            }
        }
        return dataMap;
    }

    public List<Row> mergeData(ProcessorContext context, Map<String, List<Row>> dataMap) {
        // This map save all ProcessorContexts for scattering
        Map<String, Map<String, Integer>> scatterSigns = Maps.newLinkedHashMap();

        // This list save all items for scattering
        Queue<Row> scatterItemList = Lists.newLinkedList();
        int resultLength = getReturnCount(context);
        List<Row> finalResult = Lists.newArrayListWithCapacity(resultLength);

        // Indicate the actual position to pick, we only use insertIndex[0]
        int[] currentPos = new int[]{0};

        // For item unique
        Set<String> filterSet = Sets.newHashSet();

        // Position item extData map
        Map<Integer, Map<String, String>> posExtData = getPosExtData(context);

        // ForceTop strategy
        forceTopMap.forEach((key, value) -> {
            filterLimitAddList(context, filterSet, finalResult, dataMap.get(key), currentPos, scatterSigns,
                scatterItemList, false, value, false, posExtData, key);
        });

        // get force pos strategy for certain pn
        Map<Integer, String> forcePosMap = getForcePosMapByPn(context, resultLength);

        if (enableExtraDispatch) {
            Map<Integer, String> extraDispatchMapping = context.getContextData(ConstantsFrame.CONTEXT_EXTRA_DISPATCH_CONFIG_MAPPING);
            Optional.ofNullable(extraDispatchMapping).ifPresent(e -> forcePosMap.putAll(e));
        }

        Set<String> useBackupKeys = Sets.newHashSet();
        Map<String, Object> ratioChooseData = Maps.newHashMap();
        // do insert
        for (int i = currentPos[0]; i < resultLength && currentPos[0] < resultLength; i++) {
            // Position is start from 1
            int position = i + 1;
            // get force pos dataType first
            String posDataTypeKey = MapUtils.getString(forcePosMap, position);
            // force pos don't need to scatter
            boolean needScatterValid = forceInsertItemNeedScatter;
            if (StringUtils.isEmpty(posDataTypeKey) || CollectionUtils.isEmpty(dataMap.get(posDataTypeKey))) {
                // get dataType by ratio
                posDataTypeKey = ratioChoose(context, ratioChooseData);
                // when forceStartFromPos not match, use backupList
                if (MapUtils.isNotEmpty(forceStartFromPosMap)
                    && position < forceStartFromPosMap.getOrDefault(posDataTypeKey, 0)
                    && backupMap.containsKey(posDataTypeKey)) {
                    List<String> backupList = backupMap.get(posDataTypeKey);
                    if (CollectionUtils.isNotEmpty(backupList)) {
                        posDataTypeKey = backupList.get(0);
                    }
                }
                needScatterValid = true;
            }
            Map<String, List<Row>> resultData = getDataByKey(dataMap, posDataTypeKey, useBackupKeys);
            String finalDataKey = posDataTypeKey;
            List<Row> dataList = null;
            if (MapUtils.isNotEmpty(resultData)) {
                // Map only has one row
                Map.Entry<String, List<Row>> entry = resultData.entrySet().iterator().next();
                finalDataKey = entry.getKey();
                dataList = entry.getValue();
            }
            Boolean needScatterFromConfig = scatterMap.get(finalDataKey);
            if (Objects.nonNull(needScatterFromConfig)) {
                needScatterValid = needScatterFromConfig;
            }
            boolean got = filterLimitAdd(context, filterSet, finalResult, dataList, currentPos, scatterSigns, scatterItemList, needScatterValid, posExtData, finalDataKey);
            if (cantScatterAppendInput && !got && CollectionUtils.isNotEmpty(dataList)) {
                filterLimitAdd(context, filterSet, finalResult, dataList, currentPos, scatterSigns, scatterItemList, false, posExtData, finalDataKey);
            }
        }

        writeBackupCounter(useBackupKeys);

        // Backup without scatter when finalResult not full
        for (String key : backupList) {
            filterLimitAddList(context, filterSet, finalResult, dataMap.get(key), currentPos, scatterSigns,
                scatterItemList, false, resultLength, true, posExtData, key);
        }

        // add pageWise and scatter len to algInfo
        pageWise(context, finalResult, dataSourceIndicatorMap);

        return finalResult;
    }

    protected LinkedHashMap<String, Integer> getMaxTypeCountConfigMap(String typeKey) {
        return maxTypeCountConfigForSourceMap.getOrDefault(typeKey, maxTypeCountConfigMap);
    }

    protected void writeBackupCounter(Set<String> useBackupKeys) {
        if (backupWriteCounter && CollectionUtils.isNotEmpty(useBackupKeys)) {
//            useBackupKeys.forEach(key -> ServiceFactory
//                .getTPPCounter().countSum(backupCounterKey + StringPool.UNDERSCORE + key, 1));
        }
    }

    protected Map<String, List<Row>> getDataByKey(Map<String, List<Row>> dataMap, String dataKey, Set<String> useBackupKeys) {
        Map<String, List<Row>> resultMap = Maps.newHashMapWithExpectedSize(1);
        List<Row> dataList = dataMap.get(dataKey);
        if (CollectionUtils.isEmpty(dataList)) {
            List<String> backupKeys = backupMap.get(dataKey);
            if (CollectionUtils.isNotEmpty(backupKeys)) {
                for (String backupKey : backupKeys) {
                    List<Row> backupData = dataMap.get(backupKey);
                    if (CollectionUtils.isNotEmpty(backupData)) {
                        dataList = backupData;
                        resultMap.put(backupKey, dataList);
                        useBackupKeys.add(dataKey + StringPool.UNDERSCORE + backupKey);
                        break;
                    }
                }
            }
        } else {
            resultMap.put(dataKey, dataList);
        }
        return resultMap;
    }

    protected Map<Integer, String> getForcePosMapByPn(ProcessorContext context, Integer resultLength) {
        int pn = context.getPn();
        // Priority: posStrategyMap > defaultPosStrategyMap > modIndexMap > defaultModIndexMap

        // Page pos config
        Map<Integer, String> posStrategyMap = pnPosStrategyMap.getOrDefault(pn, Maps.newLinkedHashMap());
        // Default page pos config
        Map<Integer, String> defaultPosStrategyMap = pnPosStrategyMap.getOrDefault(DEFAULT_PAGE_CONFIG_NUMBER, Maps.newLinkedHashMap());

        // Page mod index config: ( example: ImmutablePair<2, 0> => x%2 == 0 )
        Map<ImmutablePair<Integer, Integer>, String> modIndexMap = pnModStrategyMap.getOrDefault(pn, Maps.newLinkedHashMap());
        // Default page mod index config
        Map<ImmutablePair<Integer, Integer>, String> defaultModIndexMap = pnModStrategyMap.getOrDefault(DEFAULT_PAGE_CONFIG_NUMBER, Maps.newLinkedHashMap());

        Map<Integer, String> result = Maps.newLinkedHashMap(posStrategyMap);

        // If page not config position data, use default position config
        for (Map.Entry<Integer, String> entry : defaultPosStrategyMap.entrySet()) {
            result.putIfAbsent(entry.getKey(), entry.getValue());
        }
        if (MapUtils.isNotEmpty(modIndexMap) || MapUtils.isNotEmpty(defaultModIndexMap)) {
            for (int i = 1; i <= resultLength; i++) {
                // Position is start from 1
                int position = i;
                Stream.concat(modIndexMap.entrySet().stream(), defaultModIndexMap.entrySet().stream()).forEach(
                    entry -> {
                        // transform mod into pos, stored in result
                        if (entry.getKey().left != 0 && (position % entry.getKey().left == entry.getKey().right)) {
                            result.putIfAbsent(position, entry.getValue());
                        }
                    });
            }
        }
        return result;
    }

    protected Map<String, Double> getRatioMap(ProcessorContext context) {
        if (enableDynamicRatio) {
            Map<String, Double> dynamicRatio = context.getContextData(ConstantsFrame.CONTEXT_DYNAMIC_RATIO_CONFIG);
            return Maps.newLinkedHashMap(Objects.isNull(dynamicRatio) ? ratioMap : dynamicRatio);
        }
        return Maps.newLinkedHashMap(ratioMap);
    }

    protected String ratioChoose(ProcessorContext context, Map<String, Object> ratioChooseData) {
        String chooseResult = StringUtils.EMPTY;
        if (useScatterKeepRatio) {
            //ʹ�����������ѡ��Ԫ��,������Ȼ���Ʊ���
            String scatterListsKey = "randomScatterListsKeepRatio";
            String curPos = "curPos";
            List<String> scatterLists = (List<String>) ratioChooseData.get(scatterListsKey);
            if (scatterLists == null) {
                int resultLength = getReturnCount(context);
                scatterLists = ScatterUtils.scatterStrategyByRandom(context, getRatioMap(context), getForcePosMapByPn(context, resultLength), resultLength);
                ratioChooseData.put(scatterListsKey, scatterLists);
            }
            int pos = (int) ratioChooseData.getOrDefault(curPos, 0);
            if (scatterLists != null && pos < scatterLists.size()) {
                ratioChooseData.put(curPos, pos + 1);
                chooseResult = scatterLists.get(pos);
            }
            return chooseResult;
        }

        if (usePreCalcScatter) {
            //ʹ��Ԥ�ȼ���õ��ϸ��ɢ�߼�
            String preCalcScatterListsKey = "preCalcScatterLists";
            String curElePos = "curElePos";
            List<String> scatterLists = (List<String>) ratioChooseData.get(preCalcScatterListsKey);
            if (scatterLists == null) {
                int resultLength = getReturnCount(context);
                scatterLists = ScatterUtils.scatterStrategy(context, getRatioMap(context), getForcePosMapByPn(context, resultLength), resultLength);
                ratioChooseData.put(preCalcScatterListsKey, scatterLists);
            }
            int pos = (int) ratioChooseData.getOrDefault(curElePos, 0);
            if (scatterLists != null && pos < scatterLists.size()) {
                ratioChooseData.put(curElePos, pos + 1);
                chooseResult = scatterLists.get(pos);
            }
            return chooseResult;
        }

        Double score = RandomUtils.nextDouble(1);
        Double result = 0D;
        for (Map.Entry<String, Double> config : getRatioMap(context).entrySet()) {
            result += config.getValue();
            if (score < result) {
                chooseResult = config.getKey();
                break;
            }
        }
        return chooseResult;
    }

    protected void filterLimitAddList(ProcessorContext context, Set<String> filterSet, List<Row> finalResult,
                                      List<Row> itemList, int[] currentPos,
                                      Map<String, Map<String, Integer>> scatterSigns, Queue<Row> scatterItemList,
                                      boolean needScatterValid, int resultLength, boolean isBackup,
                                      Map<Integer, Map<String, String>> posExtData, String typeKey) {
        if (currentPos[0] < resultLength && CollectionUtils.isNotEmpty(itemList)) {
            if (backupWriteCounter && isBackup) {
//                ServiceFactory.getTPPCounter().countSum(backupCounterKey, 1);
            }
            int size = itemList.size();
            for (int i = 0; i < size; i++) {
                filterLimitAdd(context, filterSet, finalResult, itemList, currentPos, scatterSigns, scatterItemList,
                    needScatterValid, posExtData, typeKey);
                if (resultLength - currentPos[0] <= 0) {
                    break;
                }
            }
        }
    }

    protected boolean filterLimitAdd(ProcessorContext context, Set<String> filterSet, List<Row> finalResult,
                                     List<Row> itemList, int[] currentPos,
                                     Map<String, Map<String, Integer>> scatterSigns, Queue<Row> scatterItemList,
                                     boolean needScatterValid, Map<Integer, Map<String, String>> posExtData,
                                     String typeKey) {
        boolean gotItem = false;
        if (CollectionUtils.isNotEmpty(itemList)) {
            Iterator<Row> iterator = itemList.iterator();
            while (iterator.hasNext()) {
                Row item = iterator.next();
                if (Objects.nonNull(item.getType()) && Objects.nonNull(item.getId())) {
                    String key = Joiner.on(StringPool.UNDERSCORE).join(item.getType(), item.getId());
                    // Item should satisfy the conditions of unique and scatter
                    if (!filterSet.contains(key) && scatterValid(context, item, scatterSigns, needScatterValid, typeKey)) {
                        // add the item to the final result
                        finalResult.add(item);
                        // currentPosition number is position index + 1
                        Integer curPos = currentPos[0] + 1;
                        if (MapUtils.isNotEmpty(posExtData) && posExtData.containsKey(curPos)) {
                            item.getExtData().putAll(posExtData.getOrDefault(curPos, Collections.EMPTY_MAP));
                        }
                        if (MapUtils.isNotEmpty(getMaxTypeCountConfigMap(typeKey)) && outputScatterFlag) {
                            // add scatter algInfo
                            item.appendAlgInfo(AlgInfoKey.SCATTER_FLAG, needScatterValid);
                        }
                        // add the key for DISTINCT
                        filterSet.add(key);
                        // update the key for SCATTER
                        updateScatterSigns(context, item, scatterItemList, scatterSigns, needScatterValid, typeKey);
                        // remove cur item
                        iterator.remove();
                        // update the actual insert position
                        currentPos[0] += 1;
                        gotItem = true;
                        break;
                    }
                }
            }
        }
        return gotItem;
    }

    protected Boolean scatterValid(ProcessorContext context, Row item, Map<String, Map<String, Integer>> scatterSigns,
                                   boolean needScatterValid, String typeKey) {
        if (needScatterValid) {
            Set<String> scatterTypeSet = getMaxTypeCountConfigMap(typeKey).keySet();
            item.setFieldValue(AllFieldName.scatter_types, scatterTypeSet);
            for (Map.Entry<String, Integer> entry : getMaxTypeCountConfigMap(typeKey).entrySet()) {
                Set<String> scatterTypes = getScatterTypeValues(context, item, entry.getKey());
                for (String type : scatterTypes) {
                    Integer typeCount = scatterSigns.computeIfAbsent(entry.getKey(), key -> Maps.newHashMap()).get(type);
                    if (Objects.nonNull(typeCount) && typeCount >= entry.getValue()) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * You can overwrite this method to process the field value
     *
     * @param context processor context
     * @param row     current row
     * @param field   field name
     * @return the field value
     */
    protected Object getFieldValue(ProcessorContext context, Row row, String field) {
        return row.getFieldValue(EnumUtil.getEnum(field));
    }

    protected Set<String> getScatterTypeValues(ProcessorContext context, Row row, String field) {
        Set<String> result = Sets.newHashSet();
        Object value = getFieldValue(context, row, field);
        if (Objects.nonNull(value)) {
            if (value instanceof Collection) {
                result.addAll((Collection) value);
            } else {
                result.add(String.valueOf(value));
            }
        }
        if (Debugger.isDebug()) {
            row.getAlgInfoMap().compute(AlgInfoKeyDebug.SPLIT,
                (k, v) -> {
                    String newValue = field + StringPool.UNDERSCORE + StringUtils.join(Lists.newArrayList(result), StringPool.COMMA);
                    if (Objects.nonNull(v) && !StringUtils.contains(v.toString(), newValue)) {
                        newValue = v + StringPool.COMMA + newValue;
                    }
                    return newValue;
                });
        }
        return result;
    }

    protected void updateScatterSigns(ProcessorContext context, Row item, Queue<Row> scatterItemList,
                                      Map<String, Map<String, Integer>> scatterSigns, boolean needScatterValid, String typeKey) {
        if (!needScatterValid) {
            return;
        }
        // remove head-item's sign
        if (scatterItemList.size() > getScatterLength(context)) {
            Row headItem = scatterItemList.poll();
            Set<String> scatterTypeSet = headItem.getFieldValue(AllFieldName.scatter_types, () -> Sets.newLinkedHashSet());
            for (String scatterType : scatterTypeSet) {
                Set<String> headItemScatterTypes = getScatterTypeValues(context, headItem, scatterType);
                for (String type : headItemScatterTypes) {
                    Map<String, Integer> signs = scatterSigns.computeIfAbsent(scatterType, key -> Maps.newHashMap());
                    Integer typeCount = signs.get(type);
                    if (Objects.nonNull(typeCount) && typeCount > 1) {
                        signs.put(type, typeCount - 1);
                    } else {
                        signs.remove(type);
                    }
                }
            }
        }
        // add new-item sign
        scatterItemList.add(item);
        for (Map.Entry<String, Integer> entry : getMaxTypeCountConfigMap(typeKey).entrySet()) {
            Set<String> itemScatterTypes = getScatterTypeValues(context, item, entry.getKey());
            for (String type : itemScatterTypes) {
                Map<String, Integer> signs = scatterSigns.computeIfAbsent(entry.getKey(), key -> Maps.newHashMap());
                Integer typeCount = signs.get(type);
                if (Objects.nonNull(typeCount)) {
                    signs.put(type, typeCount + 1);
                } else {
                    signs.put(type, 1);
                }
            }
        }
    }

    protected int getReturnCount(ProcessorContext context) {
        return returnCount == -1 ? ContextUtil.getIntOrDefault(context.getTppContext(), countParam, context.getCount()) : returnCount;
    }

    protected void pageWise(ProcessorContext context, List<Row> finalResult, Map<String, String> dataSourceIndicatorMap) {
        // add pageWise and scatter len to algInfo
        if (needPageWise) {
            Integer resLen = finalResult.size() > 10 ? 10 : finalResult.size();
            String[] pageWiseList = new String[resLen];
            for (int i = 0; i < resLen; i++) {
                String dataSource = "R";
                if (finalResult.get(i) != null) {
                    Object reRankType = finalResult.get(i).getAlgInfoMap().get(AlgInfoKey.RC_RERANK_TYPE);
                    dataSource = dataSourceIndicatorMap.getOrDefault(reRankType, "R");
                }
                pageWiseList[i] = dataSource;
            }
            String pageType = StringUtils.join(pageWiseList);
            for (Row item : finalResult) {
                if (item != null) {
                    item.appendAlgInfo(AlgInfoKey.PAGE_WISE, pageType);
                    item.appendAlgInfo(AlgInfoKey.SCATTER_LEN, getScatterLength(context));
                    Object reRankType = item.getAlgInfoMap().get(AlgInfoKey.RC_RERANK_TYPE);
                    String dataSource = dataSourceIndicatorMap.getOrDefault(reRankType, "R");
                    item.appendAlgInfo(AlgInfoKey.INSERT, dataSource);
                }
            }
        }
    }
}
