package org.java.plus.dag.core.service.rank;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
//import com.taobao.recommendplatform.protocol.domain.rtpClient.RtpConfiguration;
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
import org.java.plus.dag.core.base.utils.CommonMethods;
import org.java.plus.dag.core.base.utils.Debugger;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.base.utils.StrUtils;
import org.java.plus.dag.taobao.RtpRequest;
//import org.java.plus.dag.core.base.utils.rtp.ServiceUtils;
//import com.taobao.rtp_client.Result.PBBytesAttribute;
//import com.taobao.rtp_client.Result.PBDoubleAttribute;
//import com.taobao.rtp_client.Result.PBMatchDocs;
//import com.taobao.rtp_client.Result.PBResult;
//import com.taobao.rtp_client.RtpRequest;
//import com.taobao.rtp_client.RtpResult;
//import com.taobao.rtp_client.RtpResult.RtpMessage;
//import com.taobao.rtp_client.ServerManager;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * @author seven.wxy
 * @date 2019/7/8
 */
public class RtpBaseRank extends BaseRank {
    protected static final String USER_FEATURE_PREFIX = "user:";
    protected static final String PROXY_VIP_SERVER = "com.taobao.rtp.proxy.vipserver";
    protected static final String DEFAULT_TARGET = "rank_predict";
    protected static final String ITEM_ID_ATTRIBUTE = "__@_item_id_@__";
    @ConfigInit(desc = "rtp domain name, VipServer address")
    protected String rtpDomain = PROXY_VIP_SERVER;
    @ConfigInit(desc = "rtp biz name")
    protected String rtpBiz = StringUtils.EMPTY;
    @ConfigInit(desc = "user feature data source")
    protected String userFeatureDataSource = "datasource/IGraphDataSource$userFeature";
    @ConfigInit(desc = "user feature type, pb or json")
    protected String userFeatureType = ConstantsFrame.JSON;
    @ConfigInit(desc = "ui context feature data source")
    protected String uiContextFeatureDataSource = StringUtils.EMPTY;
    @ConfigInit(desc = "rtp request timeout ms")
    protected Long rtpTimeOut = 50L;
    @ConfigInit(desc = "rtp service use cm2 config")
    protected Boolean useCm2 = true;
    @ConfigInit(desc = "rtp service cm2 domain config")
    protected String rtpCm2Domain = "rtp_cm2_youku";
    @ConfigInit(desc = "rank score result field name")
    protected String scoreField = "score";
    @ConfigInit(desc = "rank biz algInfo name")
    protected String rkBizAlgInfoName = "RK_BIZ";
    @ConfigInit(desc = "rank score algInfo name")
    protected String rkScoreAlgInfoName = "RK_SCORE";

    @ConfigInit(desc = "long feature default value")
    protected Integer intFeatureDefaultValue = 0;
    @ConfigInit(desc = "double feature default value")
    protected Double doubleFeatureDefaultValue = 0.0;
    @ConfigInit(desc = "string feature default value")
    protected String stringFeatureDefaultValue = "null";

    @ConfigInit(desc = "ui feature value type, double,int,string")
    protected String uiFeatureValueType = ConstantsFrame.DOUBLE_TYPE;
    @ConfigInit(desc = "print error request or not")
    protected boolean printErrorRequest = false;
    @ConfigInit(desc = "rtp rank item count per request")
    protected int itemCountPerRequest = 340;

    @ConfigInit(desc = "use Graph request or not")
    protected boolean graphRequest = false;
    @ConfigInit(desc = "use Graph request or not")
    protected String outputTensors = DEFAULT_TARGET;
    protected List<String> outputTensorList = Lists.newArrayList(DEFAULT_TARGET);

    @ConfigInit(desc = "tensor result key mapping to AllFieldName enum")
    protected String tensorMappingField = StringUtils.EMPTY;
    protected Map<String, AllFieldName> outputTensorMappingField = Maps.newHashMap();

    @ConfigInit(desc = "tensor result key mapping to AlgInfoKey enum")
    protected String tensorMappingAlgInfo = StringUtils.EMPTY;
    protected Map<String, AlgInfoKey> outputTensorMappingAlgInfo = Maps.newHashMap();

    @ConfigInit(desc = "user feature alias")
    protected String userFeatureAlias = StringUtils.EMPTY;
    protected Map<String, String> userFeatureAliasMap = Maps.newHashMap();

    @ConfigInit(desc = "ui feature alias")
    protected String uiFeatureAlias = StringUtils.EMPTY;
    protected Map<String, String> uiFeatureAliasMap = Maps.newHashMap();

    @Override
    public void doInit(ProcessorConfig processorConfig) {
        if (StringUtils.isNotEmpty(outputTensors)) {
            outputTensorList = StrUtils.strToList(outputTensors, StringPool.COMMA);
        }
        if (StringUtils.isNotEmpty(tensorMappingField)) {
            outputTensorMappingField = parseConfigMap(tensorMappingField, "tensor to field", new TypeReference<Map<String, AllFieldName>>() {});
        }
        if (StringUtils.isNotEmpty(tensorMappingAlgInfo)) {
            outputTensorMappingAlgInfo = parseConfigMap(tensorMappingAlgInfo, "tensor to algInfo", new TypeReference<Map<String, AlgInfoKey>>() {});
        }
        userFeatureAliasMap = parseConfigMap(userFeatureAlias, "user feature alias", new TypeReference<Map<String, String>>() {}, Maps.newHashMap());
        uiFeatureAliasMap = parseConfigMap(uiFeatureAlias, "ui feature alias", new TypeReference<Map<String, String>>() {}, Maps.newHashMap());
    }

    @Override
    public DataSet<Row> doProcess(ProcessorContext processorContext, DataSet<Row> dataSet, Map<String, DataSet<Row>> dataSetMap) {
        if (isNullOrEmpty(dataSet)) {
            return dataSet;
        }
        // get feature result
        Map<String, DataSet<Row>> featureResult = getFeatureResult(processorContext, dataSet, dataSetMap);
        // build rtp request
//        RtpRequest rtpRequest = buildRtpRequest(processorContext, dataSet, featureResult);
        if (graphRequest) {
//            buildGraphRequest(rtpRequest);
        }
//        Debugger.put(this, () -> "rtpRequest", () -> rtpRequest.toString());
//        if (Debugger.isLocal()) {
//            Logger.info(() -> "Request RTP:\n" + rtpRequest.toString());
//        }
        // call rtp service
        DataSet<Row> scoreDataSet =null;// callRtpService(processorContext, rtpRequest, async);
        // append algInfo
        scoreDataSet = scoreDataSet.appendAlgInfo(AlgInfoKey.valueOf(rkBizAlgInfoName), rtpBiz);
        // apply rtp score to DataSet
        dataSet = dataSet.leftOuterJoinOnEquals(scoreDataSet, Row::getId, Row::getId,
            (leftRow, rightRow) -> Objects.nonNull(rightRow) ? leftRow.mergeRow(rightRow) : leftRow.setFieldValue(
                AllFieldName.valueOf(scoreField), 0D));
        return dataSet;
    }

    protected Map<String, DataSet<Row>> getFeatureResult(ProcessorContext processorContext,
                                                         DataSet<Row> dataSet,
                                                         Map<String, DataSet<Row>> dataSetMap) {
        return callProcessorByConfigKeyAndValue(processorConfigKey, processorConfigValue, processorContext, dataSet, dataSetMap);
    }

//    protected void buildGraphRequest(RtpRequest request) {
//        if (CollectionUtils.isNotEmpty(outputTensorList)) {
//            request.setAttribute(outputTensorList);
//            request.setOutfmt("pb2");
//        }
//    }

//    protected Object buildRtpRequest(ProcessorContext processorContext, DataSet<Row> mainDataSet,
//                                         Map<String, DataSet<Row>> featureResult) {
//        RtpRequest request = new RtpRequest();
//        try {
//            request.setBiz(rtpBiz);
//            List<String> itemList = mainDataSet.getData().stream().map(p -> p.getId()).collect(Collectors.toList());
//            request.setItemList((List)itemList);
//            Map qInfoMap = buildUserFeature(request, processorContext, mainDataSet, featureResult);
//            buildContextFeature(request, processorContext, mainDataSet, featureResult, itemList);
//            addCustomUserContextFeature(qInfoMap, processorContext, mainDataSet, featureResult);
//            addCustomUIContextFeature(request, processorContext, mainDataSet);
//        } catch (Exception e) {
//            Debugger.exception(this, StatusType.INVOKE_RTP_EXCEPTION, e);
//            Logger.onlineWarn("RtpRequest build error," + ExceptionUtils.getStackTrace(e));
//        }
//        return request;
//    }

    protected Map buildUserFeature(RtpRequest request, ProcessorContext processorContext,
                                   DataSet<Row> mainDataSet,
                                   Map<String, DataSet<Row>> featureResult) {
        DataSet<Row> userFeature = getDataSetByProcessorConfigKey(userFeatureDataSource, processorContext, mainDataSet,
            featureResult);
        Map qInfoMap = Maps.newHashMap();
//        request.setQinfo(qInfoMap);
        try {
            if (Objects.nonNull(userFeature) && userFeature.isNotEmpty()) {
                if (Objects.equals(userFeatureType, ConstantsFrame.JSON)) {
                    Map featureQInfoMap = getJSONUserFeatureFromDataSet(processorContext, userFeature);
                    if (MapUtils.isNotEmpty(featureQInfoMap)) {
                        StringBuilder builder = new StringBuilder();
                        for (Object entry : featureQInfoMap.entrySet()) {
                            Map.Entry e = (Map.Entry)entry;
                            if (Objects.nonNull(e.getValue())) {
                                Object featureObject = e.getValue();
                                builder.setLength(0);
                                String userFeatureKey = String.valueOf(e.getKey());
                                if (MapUtils.isNotEmpty(userFeatureAliasMap)) {
                                    userFeatureKey = userFeatureAliasMap.getOrDefault(userFeatureKey, userFeatureKey);
                                }
                                String featureKey = builder.append(USER_FEATURE_PREFIX).append(userFeatureKey).toString();
                                addUserFeatureValue(processorContext, mainDataSet, featureResult, qInfoMap, featureKey, featureObject);
                            }
                        }
                    }
                } else {
                    String qInfo = getUserFeatureFromDataSet(processorContext, userFeature);
                    if (StringUtils.isNotEmpty(qInfo)) {
//                        request.setQinfo(qInfo);
                    }
                }
            }
        } catch (Exception e) {
            Debugger.exception(this, StatusType.INVOKE_RTP_EXCEPTION, e);
            Logger.onlineWarn("RtpRequest build use feature error," + ExceptionUtils.getStackTrace(e));
        }
        return qInfoMap;
    }

    protected void addUserFeatureValue(ProcessorContext processorContext,
                                       DataSet<Row> mainDataSet,
                                       Map<String, DataSet<Row>> featureResult,
                                       Map qInfoMap,
                                       String featureKey, Object featureObject) {
        String featureValue = featureObject.toString();
        if (StringUtils.contains(featureValue, StringPool.COMMA) && !StringUtils.contains(featureValue, "^")) {
            List<String> valueList = CommonMethods.COMMA_SPLITTER.splitToList(featureValue);
            qInfoMap.put(featureKey, valueList);
        } else {
            qInfoMap.put(featureKey, ImmutableList.of(featureObject));
        }
    }

    protected void buildContextFeature(RtpRequest request, ProcessorContext processorContext,
                                       DataSet<Row> mainDataSet,
                                       Map<String, DataSet<Row>> featureResult,
                                       List<String> itemList) {
        try {
            DataSet<Row> contextFeature = getDataSetByProcessorConfigKey(uiContextFeatureDataSource, processorContext,
                mainDataSet,
                featureResult);
            Map<String, JSONObject> featureJsonMap = Maps.newHashMap();
            Set<String> contextFieldsAll = Sets.newLinkedHashSet();
            Set<String> itemIds = itemList.stream().collect(Collectors.toSet());
            Optional.ofNullable(contextFeature).ifPresent(p -> p.getData().forEach(row -> {
                String itemId = row.getFieldValue(AllFieldName.item_id);
                JSONObject json = row.getFieldValue(AllFieldName.feature_json);
                if (Objects.isNull(json)) {
                    String featureJson = row.getFieldValue(AllFieldName.fea_json);
                    json = JSONObject.parseObject(featureJson);
                }
                if (Objects.nonNull(json)) {
                    Set<String> keys = json.keySet();
                    if (MapUtils.isNotEmpty(uiFeatureAliasMap)) {
                        Map addColumns = Maps.newHashMapWithExpectedSize(uiFeatureAliasMap.size());
                        Set<String> newKeys = Sets.newHashSetWithExpectedSize(keys.size());
                        for (String key : keys) {
                            String newKey = uiFeatureAliasMap.get(key);
                            if (Objects.nonNull(newKey)) {
                                addColumns.put(newKey, json.get(key));
                            } else {
                                newKey = key;
                            }
                            newKeys.add(newKey);
                        }
                        keys = newKeys;
                        json.putAll(addColumns);
                    }
                    contextFieldsAll.addAll(keys);
                    featureJsonMap.put(itemId, json);
                }
            }));

            Debugger.put(this, "features", featureJsonMap);
            if (CollectionUtils.isNotEmpty(contextFieldsAll) && CollectionUtils.containsAny(itemIds,
                featureJsonMap.keySet())) {
                List<String> contextFields = Lists.newArrayList(contextFieldsAll);
//                request.setContextFields(contextFields);
                Map<String, List<Object>> featureValues = Maps.newHashMap();
                for (int i = 0; i < itemList.size(); i++) {
                    String itemId = itemList.get(i);
                    JSONObject json = featureJsonMap.get(itemId);
                    final JSONObject finalJson = ObjectUtils.defaultIfNull(json, ConstantsFrame.NULL_JSON_OBJECT);
                    contextFields.forEach(fieldName -> {
                        Object value;
                        if (ConstantsFrame.DOUBLE_TYPE.equals(uiFeatureValueType)) {
                            value = ObjectUtils.defaultIfNull(finalJson.getDouble(fieldName),
                                doubleFeatureDefaultValue);
                        } else if (ConstantsFrame.INT_TYPE.equals(uiFeatureValueType)) {
                            value = ObjectUtils.defaultIfNull(finalJson.getInteger(fieldName), intFeatureDefaultValue);
                        } else {
                            value = ObjectUtils.defaultIfNull(finalJson.get(fieldName), stringFeatureDefaultValue);
                        }
                        featureValues.compute(itemId, (k, v) -> (v == null) ? Lists.newArrayList() : v).add(value);
                    });
                }
                featureValues.entrySet().forEach(entry -> {
                    try {
//                        request.setItemContext(entry.getKey(), entry.getValue());
                    } catch (Exception e) {
                        Logger.error(String.format("RTP UI feature parse error, %s", e.getMessage()), e);
                    }
                });
            }
        } catch (Exception e) {
            Debugger.exception(this, StatusType.INVOKE_RTP_EXCEPTION, e);
            Logger.onlineWarn("RtpRequest build context feature error," + ExceptionUtils.getStackTrace(e));
        }
    }

    protected void addCustomUIContextFeature(RtpRequest request, ProcessorContext context,
                                             DataSet<Row> mainDataSet) {
    }

    protected void addCustomUserContextFeature(Map qInfoMap, ProcessorContext context,
                                               DataSet<Row> mainDataSet, Map<String, DataSet<Row>> featureResult) {
    }

    protected String getUserFeatureFromDataSet(ProcessorContext context, DataSet<Row> userFeature) {
        return userFeature.getFirstItem().orElseGet(Row::new).getFieldValue(AllFieldName.fea_json, StringUtils.EMPTY);
    }

    protected Map getJSONUserFeatureFromDataSet(ProcessorContext context, DataSet<Row> userFeature) {
        String featureJson = userFeature.getFirstItem().orElseGet(Row::new).getFieldValue(AllFieldName.fea_json, StringUtils.EMPTY);
        return StringUtils.isEmpty(featureJson) ? ConstantsFrame.NULL_JSON_OBJECT : JSONObject.parseObject(featureJson);
    }

//    protected RtpConfiguration getRtpRequestConfiguration() {
//        RtpConfiguration config = new RtpConfiguration();
//        if (useCm2 && !Debugger.isLocal() && !Debugger.isRtpDebug()) {
//            config.setUseCm2(useCm2);
//            config.setDomain(rtpCm2Domain);
//            config.setSelectStrategy(ServerManager.SelectStrategy.SS_RANDOM);
//        } else {
//            config.setDomain(Debugger.isLocal() ? PROXY_VIP_SERVER : rtpDomain);
//        }
//        config.setItemCountPerRequest(itemCountPerRequest);
//        config.setTimeout(rtpTimeOut.intValue());
//        return config;
//    }

    protected DataSet<Row> callRtpService(ProcessorContext processorContext, RtpRequest request, boolean async) {
        DataSet<Row> scoreDataSet;
        if (async) {
            scoreDataSet = asyncCallRemoteRtpService(processorContext, request);
        } else {
            scoreDataSet = callRemoteRtpService(processorContext, request);
        }
        return scoreDataSet;
    }

    protected DataSet<Row> callRemoteRtpService(ProcessorContext processorContext, RtpRequest request) {
        // sync call rtp service
//        Object message = ServiceUtils.rtpService(processorContext, request, getRtpRequestConfiguration(), printErrorRequest, graphRequest);
        // write rtp request info to TT log stream
        writeTTContextData(processorContext, request, processorContext.isWriteRTPRequestToTT());
        // parse sync result to DataSet
        DataSet<Row> result = new DataSet<>();
        try {
//            result.setData(parseResultToDataSet(message));
        } catch (Exception e) {
//            if (printErrorRequest) {
//                Logger.onlineWarn(
//                    String.format("Inst %s Domain %s biz %s request is %s ,error: %s", this.getInstanceKey(), rtpDomain, request.getBiz(),
//                        request, ExceptionUtils.getStackTrace(e)));
//            } else {
//                Logger.onlineWarn(
//                    String.format("Inst %s Domain %s biz %s ,error: %s", this.getInstanceKey(), rtpDomain, request
//                        .getBiz(), e.getMessage()));
//            }
//            ServiceFactory.getTPPCounter().countSum(TppCounterNames.RTP_EXCEPTION.getCounterName() + "-parse-" + request.getBiz(), 1);
        }
        return result;
    }

    protected DataSet<Row> asyncCallRemoteRtpService(ProcessorContext processorContext, RtpRequest request) {
        DataSet<Row> result = new DataSet<>();
        // async call rtp service
        long start = System.currentTimeMillis();
        Future<Object> message = null;//ServiceUtils.asyncRtpService(processorContext, request, getRtpRequestConfiguration(), printErrorRequest, graphRequest);
        // define async result process function
        final Function<Object, List<Row>> transformOnCompleteFunction = (response) -> {
            List<Row> rowList = Lists.newArrayList();
            try {
                Future<Object> res = (Future<Object>)response;
                if (Objects.nonNull(res)) {
                    Object msg = res.get(rtpTimeOut, TimeUnit.MILLISECONDS);
//                    ServiceUtils.writeRtpCost(processorContext, request, System.currentTimeMillis() - start);
                    if (Objects.nonNull(msg)) {
                        rowList = parseResultToDataSet(msg);
                    }
                }
            } catch (Exception e) {
                if (printErrorRequest) {
//                    Logger.onlineWarn(
//                        String.format("Inst %s Domain %s biz %s request is %s ,error: %s", getInstanceKey(), rtpDomain, request.getBiz(),
//                            request, ExceptionUtils.getStackTrace(e)));
                } else {
//                    Logger.onlineWarn(
//                        String.format("Inst %s Domain %s biz %s,error: %s", getInstanceKey(), rtpDomain, request.getBiz(), e.getMessage()));
                }
//                ServiceFactory.getTPPCounter().countSum(TppCounterNames.RTP_EXCEPTION.getCounterName() + "-parse-" + request.getBiz(), 1);
            }
            return rowList;
        };
        // set async result to DataSet
        result.setAsyncData(message, transformOnCompleteFunction);
        // write rtp request info to TT log stream
        writeTTContextData(processorContext, request, processorContext.isWriteRTPRequestToTT());
        return result;
    }

    protected List<Row> parseResultToDataSet(Object message) throws Exception {
        return null;//graphRequest ? parsePBResultToDataSet((PBResult)message) : parseRtpMessageToDataSet((RtpMessage)message);
    }

//    protected List<Row> parsePBResultToDataSet(PBResult message) throws Exception {
//        Debugger.put(this, () -> "graphResponseErr", () -> message == null ? "NULL" : new Gson().toJson(message.getErrorResultsList()));
//        Debugger.put(this, () -> "graphResponse", () -> message == null ? "NULL" : new Gson().toJson(message.getMatchDocs()));
//        if (message != null && message.getErrorResultsCount() == 0) {
//            Map<String, Row> rowMap = Maps.newHashMap();
//            PBMatchDocs matchDocs = message.getMatchDocs();
//            int docCount = matchDocs.getNumMatchDocs();
//            PBBytesAttribute itemIdList = matchDocs.getBytesAttrValuesList().stream()
//                .filter(e -> ITEM_ID_ATTRIBUTE.equals(e.getKey()))
//                .findFirst()
//                .orElse(null);
//            if (Objects.nonNull(itemIdList)) {
//                Set<String> outputTensorSet = Sets.newHashSet(outputTensorList);
//                List<PBDoubleAttribute> returnColumns = matchDocs.getDoubleAttrValuesList()
//                    .stream().filter(e -> outputTensorSet.contains(e.getKey()))
//                    .collect(Collectors.toList());
//                if (CollectionUtils.isNotEmpty(returnColumns)) {
//                    for (int i = 0; i < docCount; i++) {
//                        String itemId = itemIdList.getBytesValue(i).toStringUtf8();
//                        Row row = rowMap.computeIfAbsent(itemId, (key) -> new Row().setId(key));
//                        for (PBDoubleAttribute column : returnColumns) {
//                            String tensor = column.getKey();
//                            Double value = column.getDoubleValue(i);
//                            AllFieldName fieldName = outputTensorMappingField.get(tensor);
//                            if (Objects.nonNull(fieldName)) {
//                                row.setFieldValue(fieldName, value);
//                            }
//                            AlgInfoKey algInfoKey = outputTensorMappingAlgInfo.get(tensor);
//                            if (Objects.nonNull(algInfoKey)) {
//                                row.appendAlgInfo(algInfoKey, value);
//                            }
//                        }
//                    }
//                }
//            }
//            return Lists.newArrayList(rowMap.values());
//        } else {
//            String msg = "PBResult is wrong!, ret msg is null=" + Objects.isNull(message) +
//                ",ret error count=" + (message == null ? "-1" : message.getErrorResultsCount() +
//                ",error msg=" + (message == null ? "-1" : message.getErrorResults(0).getErrorDescription()));
//            throw new Exception(msg);
//        }
//    }

//    protected List<Row> parseRtpMessageToDataSet(RtpMessage message) throws Exception {
//        List<Row> rowList = Lists.newArrayList();
//        if (message != null && message.getStatus() == RtpResult.RtpStatus.RS_OK) {
//            for (RtpResult.DocUnit doc : message.getUnitsList()) {
//                Row row = new Row();
//                row.setId(String.valueOf(doc.getId()));
//                row.setFieldValue(AllFieldName.valueOf(scoreField), Double.isNaN(doc.getScore()) ? -1 : doc.getScore());
//                row.appendAlgInfo(AlgInfoKey.valueOf(rkScoreAlgInfoName), Double.isNaN(doc.getScore()) ? -1 : doc.getScore());
//                rowList.add(row);
//            }
//        } else {
//            String msg = "RtpMessage is wrong!, ret msg is null=" + Objects.isNull(message) +
//                ",ret code=" + (message == null ? "-1" : message.getStatus());
//            throw new Exception(msg);
//        }
//        return rowList;
//    }

    /**
     * Overwrite this method to write rtp request info to TT
     * @param processorContext
     * @param request
     * @param writeRtpRequestToTT
     */
    public void writeTTContextData(ProcessorContext processorContext, RtpRequest request, boolean writeRtpRequestToTT) {

    }
}
