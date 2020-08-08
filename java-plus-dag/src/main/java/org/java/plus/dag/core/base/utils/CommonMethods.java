package org.java.plus.dag.core.base.utils;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSONObject;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
//import com.taobao.recommendplatform.protocol.concurrent.AsyncResult;
//import com.taobao.recommendplatform.protocol.service.AbstractServiceFactory;
//import com.taobao.recommendplatform.protocol.service.HttpClientExtend;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.constants.TppCounterNames;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.em.FieldNameEnum;
import org.java.plus.dag.core.base.em.RecallType;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.proc.AbstractProcessor;
import org.java.plus.dag.core.ds.TairDataSourceBase;
import org.springframework.scheduling.annotation.AsyncResult;

import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.util.EntityUtils;

import static org.java.plus.dag.core.base.utils.RealDebugger.CONTEXT;
import static org.java.plus.dag.core.base.utils.RealDebugger.COST;
import static org.java.plus.dag.core.base.utils.RealDebugger.DEBUG;
import static org.java.plus.dag.core.base.utils.RealDebugger.EVENT_TYPE;
import static org.java.plus.dag.core.base.utils.RealDebugger.EXCEPTION;
import static org.java.plus.dag.core.base.utils.RealDebugger.INPUT;
import static org.java.plus.dag.core.base.utils.RealDebugger.OTHER;
import static org.java.plus.dag.core.base.utils.RealDebugger.OUTPUT;
import static org.java.plus.dag.core.base.utils.RealDebugger.PARENT_ID;
import static org.java.plus.dag.core.base.utils.RealDebugger.SERVICE_ID;
import static org.java.plus.dag.core.base.utils.RealDebugger.SPAN_ID;
import static org.java.plus.dag.core.base.utils.RealDebugger.TIME;
import static org.java.plus.dag.core.base.utils.RealDebugger.TRACE_ID;
import static org.java.plus.dag.core.base.utils.RealDebugger.UUID;

/**
 * @author Created by youku on 2017/9/18.
 */
public class CommonMethods {
    public static final String TT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss,SSS";

    public static final Splitter COMMA_SPLITTER = Splitter.on(StringPool.COMMA);
    public static final Splitter COLON_SPLITTER = Splitter.on(StringPool.COLON);
    public static final Splitter EQUAL_SPLITTER = Splitter.on(StringPool.EQUALS);
    public static final Splitter PIPE_SPLITTER = Splitter.on(StringPool.PIPE);

    // feedback data and pool data config read use real tair key
    public static final Set NAME_SPACE_WHITE_LIST = Sets.newHashSet(148, 4023);

    public static String parseCacheKey(String key) {
        String result = key;
        if (StringUtils.isNotEmpty(key) && ThreadLocalUtils.isPressureTest()) {
            result = key + "eagle_eye";
        }
        return result;
    }

    public static List<String> parseCacheKeyList(List<String> keys) {
        if (CollectionUtils.isNotEmpty(keys)) {
            return keys.stream().map(CommonMethods::parseCacheKey).collect(Collectors.toList());
        }
        return Lists.newArrayList();
    }

    public static String getRtTTRecord(String type, String key, long cost) {
        String date = new SimpleDateFormat(TT_DATE_FORMAT).format(new Date());
        long appId = ThreadLocalUtils.getContext() == null ? -1L : ThreadLocalUtils.getContext().getCurrentAppId();
        return date + StringPool.TAB + appId + StringPool.TAB + type + StringPool.TAB + key + StringPool.TAB + cost;
    }

    public static boolean writeWithDiscard(String topic, String accessKey, String msg) {
        boolean result = false;
//        try {
//            result = AbstractServiceFactory.getTimeTunnelWriteService().writeWithDiscard(topic, accessKey, msg);
//        } catch (Exception e) {
//            ServiceFactory.getTPPCounter().countSum(TppCounterNames.TT_WRITE_ERROR.getCounterName() + topic, 1);
//        }
        return result;
    }

    public static boolean writeDebugDataToTT(AbstractProcessor processor, ProcessorContext processorContext,
                                             Supplier<String> inputSupplier, Supplier<DataSet<Row>> dataSetSupplier,
                                             Exception exp,
                                             long cost, String ttTopic, String ttAccesskey,
                                             boolean isReal) {
        if (!Debugger.isLocal()) {
//            ServiceFactory.getSolutionInvoker()
//                .begin(CommonMethods.class.getName())
//                .invoke("writeDebugDataToTT",
//                    () -> CommonMethods.doWriteDebugDataToTT(processor, processorContext, inputSupplier, dataSetSupplier,
//                        exp, cost, ttTopic, ttAccesskey, isReal))
//                .end();
            return true;
        } else {
            return doWriteDebugDataToTT(processor, processorContext, inputSupplier, dataSetSupplier, exp, cost, ttTopic,
                ttAccesskey, isReal);
        }
    }

    private static boolean doWriteDebugDataToTT(AbstractProcessor processor, ProcessorContext processorContext,
                                                Supplier<String> inputSupplier, Supplier<DataSet<Row>> dataSetSupplier,
                                                Exception exp,
                                                long cost, String ttTopic, String ttAccesskey,
                                                boolean isReal) {
        boolean isEnable = processor.isEnableProcessor();
        String inputStr = !isEnable ? inputSupplier.get() : StringUtils.EMPTY;
        inputSupplier = null;
        String output = !isEnable ? inputStr : RealDebugger.transformDataSet(dataSetSupplier.get());
        dataSetSupplier = null;
        String e = exp == null ? StringUtils.EMPTY : ExceptionUtils.getStackTrace(exp);
        exp = null;
        String requestId = processorContext.getRequestId();
        String traceId = isReal ? processorContext.getRequestId() : processorContext
            .getUtdid() + StringPool.UNDERSCORE + processorContext.getRealAppId() + StringPool.UNDERSCORE + System
            .currentTimeMillis();
        processorContext = null;
        String context = processor.getProcessorConfig().toString();
        String eventType = RealDebugger.getEventType(processor).name();
        String spanId = processor.getInstanceKey();
        processor = null;
        JSONObject writeToTT = new JSONObject(14);
        writeToTT.put(TRACE_ID, traceId);
        writeToTT.put(INPUT, inputStr);
        writeToTT.put(OUTPUT, output);
        writeToTT.put(COST, cost);
        writeToTT.put(EXCEPTION, e);
        writeToTT.put(DEBUG, StringUtils.EMPTY);
        writeToTT.put(CONTEXT, context);
        writeToTT.put(OTHER, StringUtils.EMPTY);
        writeToTT.put(SPAN_ID, spanId);
        writeToTT.put(PARENT_ID, StringUtils.EMPTY);
        writeToTT.put(EVENT_TYPE, eventType);
        writeToTT.put(SERVICE_ID, "youku");
        writeToTT.put(TIME, System.currentTimeMillis());
        if (!isReal) {
            writeToTT.put(UUID, requestId);
        }
        return writeWithDiscard(ttTopic, ttAccesskey, writeToTT.toString());
    }

    /**
     * key=tagId, value=tagName
     *
     * @param input
     * @return
     */
    public static Map<String, String> splittingWithSplitter(String input) {
        return splittingWithSplitter(input, false);
    }

    /**
     * @param input
     * @param revertKeyValue true:key=tagName, value=tagId. false:key=tagId, value=tagName
     * @return
     */
    public static Map<String, String> splittingWithSplitter(String input, boolean revertKeyValue) {
        return splittingWithSplitter(input, COMMA_SPLITTER, EQUAL_SPLITTER, revertKeyValue);
    }

    public static Map<String, String> splittingWithSplitter(String input, Splitter firstSplit, Splitter secondSplit,
                                                            boolean revertKeyValue) {
        Map<String, String> resultMap = Maps.newHashMap();
        if (StringUtils.isNotBlank(input)) {
            Iterable<String> array = firstSplit.split(input);
            for (String str : array) {
                List<String> idName = secondSplit.splitToList(str);
                if (idName.size() > 1) {
                    if (revertKeyValue) {
                        //key=tagName, value=tagId
                        resultMap.put(idName.get(1), idName.get(0));
                    } else {
                        //key=tagId, value=tagName
                        resultMap.put(idName.get(0), idName.get(1));
                    }
                }
            }
        } else {
            resultMap = new HashMap<>(0);
        }
        return resultMap;
    }

    public static Map<String, String> revertMap(Map<String, String> input) {
        Map<String, String> result = Maps.newHashMap();
        for (Map.Entry<String, String> entry : input.entrySet()) {
            if (entry.getValue() != null) {
                result.put(entry.getValue(), entry.getKey());
            }
        }
        return result;
    }

    public static DataSet<Row> constructDataSet(String key, String value) {
        DataSet<Row> newDataSet = new DataSet<>();
        Map<AllFieldName, Object> dataMap = Maps.newEnumMap(AllFieldName.class);
        dataMap.put(AllFieldName.tair_key, key);
        dataMap.put(AllFieldName.tair_value, value);
        newDataSet.setData(Lists.newArrayList(new Row(dataMap)));
        return newDataSet;
    }

    public static String getCacheKeyPrefix(ProcessorContext context, String channelKey,
                                           Boolean channelKeyAppendAppId, Boolean channelKeyAppendContentId,
                                           List<String> channelKeyAppendParamKeys) {
        String ck = channelKey;
        if (channelKeyAppendAppId) {
            ck = channelKey + StringPool.UNDERSCORE + context.getAppId();
        }
        if (channelKeyAppendContentId) {
            ck = ck + StringPool.UNDERSCORE + context.getTppContext().get(ConstantsFrame.CONTENT_ID);
        }
        if (CollectionUtils.isNotEmpty(channelKeyAppendParamKeys)) {
            List<String> values = channelKeyAppendParamKeys.stream()
                .map(e -> ContextUtil.getStringOrDefault(context.getTppContext(), e, StringUtils.EMPTY))
                .filter(e -> StringUtils.isNotEmpty(e))
                .collect(Collectors.toList());
            String result = StringUtils.join(values, StringPool.UNDERSCORE);
            if (StringUtils.isNotEmpty(result)) {
                ck = ck + StringPool.UNDERSCORE + result;
            }
        }
        String channelKeyDebug = ck + "_debug";
        Boolean forceDebugCache = ContextUtil.getBooleanOrDefault(context.getTppContext(), "forceDebugCache", false);
        return context.getDebug() && BooleanUtils.isTrue(forceDebugCache) ? channelKeyDebug : ck;
    }

    public static String buildUserCacheKey(String channelKey, String utdid) {
        return ConstantsFrame.CHANNEL_SESSION_PREFIX + StringPool.UNDERSCORE + channelKey + StringPool.UNDERSCORE + utdid;
    }

    public static DataSet<Row> getTairDataWithpKeysKey(ProcessorContext processorContext, String dataSourceConfigKey,
                                                       String pKey, String sKey) {
        TairDataSourceBase tairDataSource = getTairDataSource(dataSourceConfigKey, StringUtils.EMPTY);
        DataSet<Row> dataSet = null;
        if (tairDataSource != null) {
            dataSet = tairDataSource.read(processorContext, pKey, sKey);
        }
        return dataSet == null ? new DataSet<>() : dataSet;
    }

    public static DataSet<Row> getSessionParseData(ProcessorContext processorContext, String dataSourceConfigKey,
                                                   String cacheKey, String valueSplitRule, String itemType) {
        DataSet<Row> session = processorContext.getContextData(cacheKey);
        if (Objects.isNull(session)) {
            DataSet<Row> tairData = getTairData(processorContext, dataSourceConfigKey, cacheKey);
            session = DataParseUtils.parseResult(tairData, valueSplitRule, AllFieldName.tair_value.toString(),
                String.valueOf(itemType), Lists.newArrayList());
            processorContext.addContextData(cacheKey, session);
        }
        return session;
    }

    public static DataSet<Row> getTairData(ProcessorContext processorContext, String dataSourceConfigKey,
                                           String cacheKey) {
        TairDataSourceBase tairDataSource = getTairDataSource(dataSourceConfigKey, StringUtils.EMPTY);
        DataSet<Row> dataSet = null;
        if (tairDataSource != null) {
            dataSet = tairDataSource.read(processorContext, Lists.newArrayList(cacheKey));
        }
        return dataSet == null ? new DataSet<>() : dataSet;
    }

    public static DataSet<Row> getTairDataByPkeys(ProcessorContext processorContext, String dataSourceConfigKey,
                                                  List<String> pkeys) {
        TairDataSourceBase tairDataSource = getTairDataSource(dataSourceConfigKey, StringUtils.EMPTY);
        DataSet<Row> dataSet = null;
        if (tairDataSource != null) {
            dataSet = tairDataSource.read(processorContext, pkeys);
        }
        return dataSet == null ? new DataSet<>() : dataSet;
    }

    public static boolean deleteTairData(ProcessorContext processorContext, String ldbDataSourceConfigKey,
                                         List<String> keyList) {
        TairDataSourceBase tairDataSource = getTairDataSource(ldbDataSourceConfigKey, StringUtils.EMPTY);
        boolean deleteResult = false;
        if (tairDataSource != null) {
            deleteResult = tairDataSource.delete(processorContext, keyList);
        }
        return deleteResult;
    }

    public static String getLdbConfigName(String ldbDataSourceConfigKey) {
        String ldbConfigName = StringUtils.EMPTY;
        if (ldbDataSourceConfigKey.contains(StringPool.DOLLAR)) {
            ldbConfigName = ldbDataSourceConfigKey.substring(ldbDataSourceConfigKey.lastIndexOf(StringPool.DOLLAR) + 1);
        }
        return ldbConfigName;
    }

    public static String replaceLdbConfigName(String ldbDataSourceConfigKey, String newLdbConfigName) {
        String newLdbDataSourceConfigKey = ldbDataSourceConfigKey;
        if (ldbDataSourceConfigKey.contains(StringPool.DOLLAR) && !newLdbConfigName.isEmpty()) {
            String oldLdbConfigName = getLdbConfigName(ldbDataSourceConfigKey);
            newLdbDataSourceConfigKey = ldbDataSourceConfigKey.replace(oldLdbConfigName, newLdbConfigName);
        }
        return newLdbDataSourceConfigKey;
    }

    public static TairDataSourceBase getTairDataSource(String ldbDataSourceConfigKey, String newLdbConfigName) {
        String newLdbDataSourceConfigKey = replaceLdbConfigName(ldbDataSourceConfigKey, newLdbConfigName);
        return TppObjectFactory.getBean(newLdbDataSourceConfigKey, TairDataSourceBase.class);
    }

    public static Map<String, String> getTagIdToNameMapFromRow(Row row) {
        Map<String, String> tagMap = row.getFieldValue(AllFieldName.tagIdNameMap);
        if (Objects.isNull(tagMap)) {
            tagMap = CommonMethods.splittingWithSplitter(row.getFieldValue(AllFieldName.tag), COMMA_SPLITTER,
                COLON_SPLITTER, false);
        }
        return tagMap;
    }

    public static Map<String, String> getTagNameToIdMapFromRow(Row row) {
        Map<String, String> tagMap = row.getFieldValue(AllFieldName.tagNameIdMap);
        if (Objects.isNull(tagMap)) {
            tagMap = CommonMethods.splittingWithSplitter(row.getFieldValue(AllFieldName.tag), COMMA_SPLITTER,
                COLON_SPLITTER, true);
        }
        return tagMap;
    }

    public static String getExtValue(String column, String input) {
        return splittingWithSplitter(input).get(column);
    }

    public static boolean isExploreRecall(Row item) {
        int recallType = item.getRecallType().order;
        return recallType == RecallType.TAG_EXPLORE.getOrder()
            || recallType == RecallType.COLD_START_ITEM.getOrder()
            || recallType == RecallType.COLD_START_TAG.getOrder();
    }

    public static boolean isOGCExploreRecall(Row item) {
        int recallType = item.getRecallType().order;
        return recallType == RecallType.REALTIME_SHOW2SHOW.getOrder()
            || recallType == RecallType.SHOW2SHOW.getOrder();
    }

    public static boolean isTag2TagRecall(Row item) {
        int recallType = item.getRecallType().order;
        return recallType == RecallType.TAG2TAG.getOrder();
    }

    public static boolean isFidRecall(Row item) {
        int recallType = item.getRecallType().order;
        return recallType == RecallType.FID_TAG2I.getOrder()
            || recallType == RecallType.FID_I2I.getOrder();
    }

    public static boolean isRelateRecall(Row item) {
        int recallType = item.getRecallType().order;
        return recallType == RecallType.OFFLINE_SRCH.getOrder()
            || recallType == RecallType.REALTIME_SRCH.getOrder();
    }

    public static String getCounterKey(String prefix, String instanceKey) {
        instanceKey = defaultIfNull(instanceKey, () -> StringUtils.EMPTY);
        int end = instanceKey.length();
        int start = StringUtils.indexOf(instanceKey, StringPool.HASH);
        start = start == -1 ? 0 : start + 1;
        return prefix + StringUtils.replaceEach(
            StringUtils.substring(instanceKey, start, end),
            new String[]{StringPool.SLASH, StringPool.DOLLAR, StringPool.DOT},
            new String[]{StringPool.UNDERSCORE, StringPool.UNDERSCORE, StringPool.UNDERSCORE});
    }

    public static <T> T defaultIfNull(final T object, Supplier<T> defaultSupplier) {
        return object != null ? object : defaultSupplier.get();
    }

    public static boolean disableTimeout() {
        return Debugger.isDebug() && !Debugger.containsCost();
    }

    public static boolean isTimeoutWithExpect(ProcessorContext context, long expectTimeout) {
        return !disableTimeout() && alreadyCost(context) > expectTimeout;
    }

    public static boolean isTimeout(ProcessorContext context) {
        return !disableTimeout() && alreadyCost(context) > context.getSceneTimeout();
    }

    public static long alreadyCost(ProcessorContext context) {
        return System.currentTimeMillis() - context.getStartTime();
    }

    public static long remainingTime(ProcessorContext context) {
        return context.getSceneTimeout() - alreadyCost(context);
    }

    public static Row removeFieldValueFluent(Row row, @NonNull FieldNameEnum fieldName) {
        Map<FieldNameEnum, Object> data = Maps.newHashMap(row.data());
        data.remove(fieldName);
        return new Row(data);
    }

    public static String http(String request, long timeout) throws Exception {
        if (!Debugger.isLocal()) {
//            HttpClientExtend hc = ServiceFactory.getHttpClientExtend();
            AsyncResult<HttpResponse> response = null;//hc.asyncGet(request, true, timeout);
            HttpResponse httpResponse = response.get();//.getResult();
            if (httpResponse != null) {
                StatusLine statusLine = httpResponse.getStatusLine();
                HttpEntity entity = httpResponse.getEntity();
                if (statusLine.getStatusCode() != 200) {
                    EntityUtils.consume(entity);
                    throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
                }
                return entity == null ? StringUtils.EMPTY : StringUtils.trimToEmpty(EntityUtils.toString(entity, StandardCharsets.UTF_8));
            }
        } else {
//            return ServiceFactory.getHttpClient().doGet(request, true);
        }
        return StringUtils.EMPTY;
    }
}
