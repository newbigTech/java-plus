package org.java.plus.dag.core.service.recall;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.java.plus.dag.core.base.annotation.ConfigInit;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.em.AlgInfoKey;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.em.RecallType;
import org.java.plus.dag.core.base.exception.StatusType;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.Debugger;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.base.utils.StrUtils;
import org.java.plus.dag.core.ds.BEDataSourceBase;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

/**
 * @author seven.wxy
 * @date 2019/7/8
 */
public class BeBaseRecall extends BaseRecall {
    private static final String DEFAULT_INT = "0";
    private static final String Q_INFO = "qinfo";
    private static final String STATIC_Q_INFO = "static_qinfo";
    private static final String Q_INFO_PREFIX = "user:";
    private static final String BE_DATA_SOURCE = "core/ds/BEDataSourceBase";
    private static final String PUBLISH_ID = "publishid";

    @ConfigInit(desc = "match type to recall type mapping")
    @Setter
    protected String matchTypeMapping = StringUtils.EMPTY;
    @ConfigInit(desc = "need user feature or not")
    protected boolean needUserFeature = false;
    @ConfigInit(desc = "if use static qinfo for be rtp recall")
    protected boolean useStaticQinfo = false;
    @ConfigInit(desc = "recall result type default value")
    protected int defaultType = 1;
    @ConfigInit(desc = "default match type")
    protected String defaultMatchType = StringUtils.EMPTY;
    @ConfigInit(desc = "be DataSource config key")
    protected String beKey = "Solution_datasource_be#datasource/BEDataSource$berecall";
    @ConfigInit(desc = "user feature iGraph config key")
    protected String userFeatureIgraphKey = "Solution_datasource_igraph#datasource/IGraphDataSource$berecallUserFeature";
    @ConfigInit(desc = "trigger id field")
    protected String triggerIdField = "vdo_id_a";
    @ConfigInit(desc = "recall result id field")
    protected String recallIdField = "vdo_id_b";
    @ConfigInit(desc = "qinfo key")
    protected String qinfoKey = "";
    @ConfigInit(desc = "default trigger id")
    protected String triggerIdDefault = StringUtils.EMPTY;

    @ConfigInit(desc = "be config keys")
    protected String beConfigKeys = StringUtils.EMPTY;
    @ConfigInit(desc = "trigger's key")
    protected String triggerKey = "Solution_processor_betrigger#engine/dag/DAGEngineProcessor$betrigger";
    protected Set<String> beConfigKeySet = Sets.newHashSet();

    @ConfigInit(desc = "user feature name white list")
    protected String userFeaNameWhitelist = StringUtils.EMPTY;
    protected Set<String> userFeaNameWhitelistSet = Sets.newHashSet();

    /**
     * constant, matchTypeMap's key is be match type,value is alg match type
     */
    @Getter
    protected Map<Integer,String> matchTypeMap = Maps.newHashMap();

    @Override
    public void doInit(ProcessorConfig processorConfig) {
        try {
            if (StringUtils.isNotBlank(matchTypeMapping)) {
                JSONObject matchTypeJson = JSONObject.parseObject(matchTypeMapping);
                for(String key : matchTypeJson.keySet()){
                    List<Integer> matchTypeList = (List<Integer>)matchTypeJson.get(key);
                    for(Integer beMatchType : matchTypeList) {
                        matchTypeMap.put(beMatchType, key);
                    }
                }
            }

            if (StringUtils.isNotBlank(beConfigKeys)){
                beConfigKeySet = StrUtils.strToStrSet(beConfigKeys, StringPool.COMMA);
            }

            if (StringUtils.isNotBlank(userFeaNameWhitelist)){
                userFeaNameWhitelistSet = StrUtils.strToStrSet(userFeaNameWhitelist, StringPool.COMMA);
            }
        } catch (Exception e) {
            Debugger.exception(this, StatusType.PARAM_PARSE_EXCEPTION, e);
        }
    }

    @Override
    public DataSet<Row> doProcess(ProcessorContext processorContext, DataSet<Row> mainDataSet, Map<String, DataSet<Row>> dataSetMap) {
        //step1:build be request config map
        Map<String,String> configMap = buildBeRequestMap(processorContext, mainDataSet, dataSetMap);
        //step2:request be to get response
        BEDataSourceBase beDataSource = getProcessorByKeyAndConfigValue(BE_DATA_SOURCE, beKey, BEDataSourceBase.class);
        beDataSource.setParentInstanceKey(this.getInstanceKey());
        DataSet beResponse = beDataSource.read(processorContext, configMap);
        //step3:parse response by config
        return parseBeResponse(beResponse, processorContext, mainDataSet, dataSetMap);
    }

    private String urlEncode(String str) {
        try {
            return URLEncoder.encode(str, "utf-8");
        } catch (Exception e) {
            Logger.warn(() -> "[BERecall] url encode failed, " + e.toString());
        }
        return StringUtils.EMPTY;
    }

    /**
     * build be request map by config
     * @param processorContext
     * @return Map
     */
    protected Map<String, String> buildBeRequestMap(ProcessorContext processorContext, DataSet<Row> dataSet, Map<String, DataSet<Row>> dataSetMap){
        Map<String, String> requestMap = Maps.newHashMap();
        Map<String, String> qInfoMap = Maps.newHashMap();
        if (StringUtils.isNotBlank(qinfoKey)){
            DataSet<Row> qInfo = dataSetMap.get(qinfoKey);
            if(!isNullOrEmpty(qInfo)){
                Row row = qInfo.getData().get(0);
                qInfoMap.putAll(row.getFieldValue(AllFieldName.QinfoMap, () -> Maps.newHashMap()));
            }
        }
        if (needUserFeature){
            qInfoMap.putAll(getUserFeature(processorContext, dataSet, dataSetMap));
        }

        // subclass can overwrite to extend
        addExtParamsToQInfoMap(processorContext, dataSet, dataSetMap, qInfoMap);

        if (!qInfoMap.isEmpty()) {
            if (useStaticQinfo) {
                Map<String, String> staticQInfoMap = new HashMap<>(qInfoMap.size());
                qInfoMap.forEach((k, v) -> staticQInfoMap.put(Q_INFO_PREFIX + k, v));
                String qInfoStr = StrUtils.map2Str(staticQInfoMap, StringPool.HASH, StringPool.SEMICOLON);
                requestMap.put(STATIC_Q_INFO, urlEncode(qInfoStr));
            } else {
                String qInfoStr = StrUtils.map2Str(qInfoMap, StringPool.COLON, StringPool.SEMICOLON);
                requestMap.put(Q_INFO, qInfoStr);
            }
        }
        // subclass can overwrite to extend
        addParamsToRequestMap(processorContext, dataSet, dataSetMap, requestMap);

        if (!beConfigKeySet.isEmpty()){
            if (dataSetMap.containsKey(triggerKey)){
                List<Row> rowList = dataSetMap.get(triggerKey).getData();
                for(Row row : rowList){
                    String key = row.getFieldValue(AllFieldName.betriggerKey);
                    if (beConfigKeySet.contains(key)){
                        requestMap.put(key, row.getFieldValue(AllFieldName.betriggerValue));
                    }
                }
            }
        }
        return requestMap;
    }

    /**
     * get user feature from iGraph
     * @param processorContext
     * @return Map
     */
    private Map<String,String> getUserFeature(ProcessorContext processorContext, DataSet<Row> dataSet, Map<String, DataSet<Row>> dataSetMap){
        Map<String, String> qInfoMap = Maps.newHashMap();
        DataSet featureRes = getDataSetByProcessorConfigKey(userFeatureIgraphKey, processorContext, dataSet, dataSetMap);
        if (!isNullOrEmpty(featureRes)){
            List<Row> rowList = featureRes.getData();
            Row row = rowList.get(0);
            String qInfo = row.getFieldValue(AllFieldName.fea_json);
            if (StringUtils.isNotEmpty(qInfo)) {
                JSONObject json = JSONObject.parseObject(qInfo);
                for (String featureName : json.keySet()) {
                    if (CollectionUtils.isNotEmpty(userFeaNameWhitelistSet)) {
                        if (!userFeaNameWhitelistSet.contains(featureName)) {
                            continue;
                        }
                    }
                    String featureValue = json.getString(featureName);
                    qInfoMap.put(featureName, StrUtils.getOrDefault(featureValue, DEFAULT_INT));
                }
            }
        }
        return qInfoMap;
    }

    /**
     * add ext request params to qInfo map, qInfo map value will append to "qinfo" param key
     * @param processorContext
     * @param dataSet
     * @param dataSetMap
     * @param qInfoMap
     */
    protected void addExtParamsToQInfoMap(ProcessorContext processorContext, DataSet<Row> dataSet,
                                          Map<String, DataSet<Row>> dataSetMap,
                                          Map<String, String> qInfoMap) {
    }

    /**
     * add ext request params to request paramMap
     * @param processorContext
     * @param dataSet
     * @param dataSetMap
     * @param requestMap
     */
    protected void addParamsToRequestMap(ProcessorContext processorContext, DataSet<Row> dataSet,
                                         Map<String, DataSet<Row>> dataSetMap,
                                         Map<String, String> requestMap) {
    }

    /**
     * parse be response
     * @param beResponse
     * @return DataSet
     */
    private DataSet<Row> parseBeResponse(DataSet beResponse,
                                         ProcessorContext processorContext,
                                         DataSet<Row> mainDataSet,
                                         Map<String, DataSet<Row>> dataSetMap){
        List<Row> beResponseRow = beResponse.getData();
        if(beResponseRow.isEmpty()){
            return new DataSet<>();
        }

        List<Row> rowList = new ArrayList<>();
        Set<String> recallIdSet = Sets.newHashSet();
        int index = 0;

        Map<String, Object> tempParamMap = Maps.newHashMap();
        for(Row row : beResponseRow){
            String recallId = row.getFieldValue(AllFieldName.valueOf(recallIdField),StringUtils.EMPTY).toString();
            if (StringUtils.isBlank(recallId) || recallIdSet.contains(recallId)) {
                continue;
            }
            String triggerId = row.getFieldValue(AllFieldName.valueOf(triggerIdField),StringUtils.EMPTY).toString();
            if(StringUtils.isBlank(triggerId)){
                triggerId = triggerIdDefault;
            }
            if (StringUtils.isNotBlank(triggerId) && StringUtils.isNotBlank(recallId)) {
                double weight = row.getFieldValue(AllFieldName.weight,0.0D);
                double reduceModelScore = row.getFieldValue(AllFieldName.__score__,0.0D);
                int type = row.getFieldValue(AllFieldName.type, defaultType);
                Integer matchType = row.getFieldValue(AllFieldName.match_type, -1);

                Row resultRow = new Row();
                resultRow.setId(recallId);
                resultRow.setFieldValue(AllFieldName.triggerItem, triggerId);
                resultRow.setTriggerId(triggerId);
                resultRow.setFieldValue(AllFieldName.triggerItemId, NumberUtils.toLong(triggerId, 0));
                resultRow.setFieldValue(AllFieldName.recallCore, weight);
                resultRow.setScore(reduceModelScore);
                resultRow.setType(type);

                if (StringUtils.equals(triggerIdField, PUBLISH_ID)) {
                    resultRow.appendAlgInfo(AlgInfoKey.RC_TYPE, RecallType.PUBLISHID2I.name());
                }
                resultRow.appendAlgInfo(AlgInfoKey.RC_TRIG, triggerId);
                resultRow.appendAlgInfo(AlgInfoKey.RC_SCORE, weight);
                resultRow.appendAlgInfo(AlgInfoKey.RM_SCORE, reduceModelScore);

                if(Objects.nonNull(matchType)) {
                    resultRow.setFieldValue(AllFieldName.match_type, matchType);
                    resultRow.appendAlgInfo(AlgInfoKey.RC_BE_TYPE, matchType);
                }

                String algMatchType = matchTypeMap.getOrDefault(matchType, defaultMatchType);
                if(StringUtils.isNotBlank(algMatchType)) {
                    resultRow.appendAlgInfo(AlgInfoKey.RC_TYPE, algMatchType);
                    resultRow.setFieldValue(AllFieldName.recallType, RecallType.valueOf(algMatchType));
                }

                // subclass can overwrite to extend
                parseItemExtAlgInfo(processorContext, mainDataSet, dataSetMap, tempParamMap, triggerId, row, resultRow);

                String publishId = row.getFieldValue(AllFieldName.publishid, StringUtils.EMPTY);
                Long triggerNum = row.getFieldValue(AllFieldName.trigger_num);
                String matchTypeList = row.getFieldValue(AllFieldName.match_type_list, StringUtils.EMPTY);
                resultRow.appendAlgInfo(AlgInfoKey.RC_RN, index + 1);
                if(StringUtils.isNotBlank(publishId)) {
                    resultRow.setFieldValue(AllFieldName.publishid, publishId);
                }
                if(Objects.nonNull(triggerNum)) {
                    resultRow.appendAlgInfo(AlgInfoKey.RC_TRIG_NUM, triggerNum);
                }
                if(StringUtils.isNotBlank(matchTypeList)) {
                    resultRow.appendAlgInfo(AlgInfoKey.RC_BETYPE_LIST, matchTypeList);
                }
                rowList.add(resultRow);
            }
            index++;
        }
        return new DataSet<>(rowList);
    }

    /**
     * parse ext info or algInfo to resultRow
     * @param processorContext
     * @param mainDataSet
     * @param dataSetMap
     * @param tempParamMap use to tmp vars out loop
     * @param triggerId
     * @param originRow
     * @param resultRow
     */
    protected void parseItemExtAlgInfo(ProcessorContext processorContext,
                                       DataSet<Row> mainDataSet,
                                       Map<String, DataSet<Row>> dataSetMap,
                                       Map<String, Object> tempParamMap,
                                       String triggerId,
                                       Row originRow,
                                       Row resultRow) {

    }
}
