//package org.java.plus.dag.core.base.utils.tair;
//
//import java.io.Serializable;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.LinkedHashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import java.util.stream.Collectors;
//
//import com.google.common.collect.Maps;
////import com.taobao.recommendplatform.protocol.counter.TPPCounter;
////import com.taobao.recommendplatform.protocol.service.ServiceFactory;
//import org.java.plus.dag.core.base.constants.ConstantsFrame;
//import org.java.plus.dag.core.base.constants.TppCounterNames;
//import org.java.plus.dag.core.base.model.ProcessorContext;
//import org.java.plus.dag.core.base.utils.CommonMethods;
//import org.java.plus.dag.core.base.utils.Debugger;
//import org.java.plus.dag.core.base.utils.Logger;
//import org.java.plus.dag.core.ds.model.TairDataSourceConfig;
//import com.taobao.tair.DataEntry;
//import com.taobao.tair.Result;
//import com.taobao.tair.ResultCode;
//import com.taobao.tair.etc.KeyValuePack;
//import com.taobao.tair.impl.mc.MultiClusterTairManager;
//import lombok.Getter;
//import org.apache.commons.collections4.CollectionUtils;
//import org.apache.commons.collections4.MapUtils;
//import org.apache.commons.lang.StringUtils;
//import org.apache.commons.lang3.ObjectUtils;
//import org.apache.commons.lang3.exception.ExceptionUtils;
//
///**
// * Created by youku on 2018/4/24.
// */
//public class TairClientImpl implements TairClient {
//    private static final TPPCounter COUNTER = ServiceFactory.getTPPCounter();
//    private static final int ERR_NAME_SPACE = -1;
//    private static final int INIT_RETRY_TIMES = 3;
//    public static final String TEST_UNIT = "daily";
//    public static final String TAIR_SAVE_RETRY_TIME_KEY = "tairSaveRetryTime";
//    public static final String TAIR_SAVE_RETRY_TIME_DEFAULT = "10";
//
//    private MultiClusterTairManager mcTairManager;
//    private int nameSpace;
//    private int timeout;
//    private String userName;
//    private String unit;
//    /**
//     * userName -> client
//     */
//    @Getter
//    private static Map<String, TairClientImpl> clientMap = Maps.newConcurrentMap();
//
//    public static TairClientImpl getInstance(TairDataSourceConfig tairConfig) {
//        String userName = tairConfig.getUserName();
//        TairClientImpl tairClientImpl = clientMap.get(userName);
//        if (Objects.isNull(tairClientImpl)) {
//            synchronized ("syn") {
//                tairClientImpl = clientMap.get(userName);
//                if (Objects.isNull(tairClientImpl)) {
//                    tairClientImpl = new TairClientImpl(tairConfig);
//                    clientMap.put(userName, tairClientImpl);
//                }
//            }
//        }
//        return tairClientImpl;
//    }
//
//    private TairClientImpl(TairDataSourceConfig tairDataSourceConfig) {
//        this.init(tairDataSourceConfig);
//    }
//
//    private void checkConfig(TairDataSourceConfig processorConfig) {
//        timeout = processorConfig.getTimeOut();
//        userName = processorConfig.getUserName();
//        nameSpace = processorConfig.getNameSpace();
//        unit = processorConfig.getUnit();
//
//        if (StringUtils.isBlank(userName) || nameSpace == ERR_NAME_SPACE) {
//            throw new IllegalArgumentException("required userName and namespace");
//        }
//    }
//
//    private void init(TairDataSourceConfig tairDataSourceConfig) {
//        checkConfig(tairDataSourceConfig);
//        for (int i = 0; i < INIT_RETRY_TIMES; i++) {
//            try {
//                mcTairManager = new MultiClusterTairManager();
//                mcTairManager.setTimeout(timeout);
//                if (Debugger.isLocal()) {
//                    mcTairManager.setUnit(TEST_UNIT);
//                } else {
//                    mcTairManager.setUnit(unit);
//                }
//                mcTairManager.setUserName(userName);
//                mcTairManager.init();
//                clientMap.put(userName, this);
//                //Logger.info(() -> nameSpace + " ldb init success!");
//                break;
//            } catch (Exception e) {
//                COUNTER.countSum(TppCounterNames.TAIR_INIT_EXCEPTION.getCounterName() + "_" + nameSpace, 1);
//                Logger.error(getErrorMessage(nameSpace + " ldb init failed"), e);
//            }
//        }
//    }
//
//    @Override
//    public boolean deleteData(String key) {
//        if (StringUtils.isBlank(key)) {
//            Logger.onlineWarn("Tair key is empty!");
//        }
//        ResultCode code = null;
//        try {
//            key = CommonMethods.parseCacheKey(key);
//            code = mcTairManager.invalid(nameSpace, key);
//        } catch (Exception e) {
//            Logger.onlineWarn(getErrorMessage(key, code, "Exception in Tair delete data"));
//            return false;
//        }
//        if (Objects.isNull(code) || !code.isSuccess()) {
//            Logger.onlineWarn(getErrorMessage(key, code, "Tair delete data failed"));
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public boolean deleteDataBatch(List<String> keys) {
//        if (CollectionUtils.isEmpty(keys)) {
//            Logger.onlineWarn("Tair key list is empty!");
//            return false;
//        }
//        ResultCode code = null;
//        try {
//            keys = CommonMethods.parseCacheKeyList(keys);
//            code = mcTairManager.minvalid(nameSpace, keys);
//        } catch (Exception e) {
//            Logger.onlineWarn(getErrorMessage(keys.toString(), code, "Exception in Tair batch delete data"));
//            return false;
//        }
//        if (Objects.isNull(code) || !code.isSuccess()) {
//            Logger.onlineWarn(getErrorMessage(keys.toString(), code, "Tair batch delete data failed"));
//            return false;
//        }
//        return true;
//    }
//
//    /**
//     * get key data from ldb
//     *
//     * @param pkeys
//     * @return
//     */
//    @Override
//    public Map<String, Serializable> getData(ProcessorContext context, List<String> pkeys) {
//        List<DataEntry> entries = getDataReturnDataEntry(context, pkeys);
//        Map<String, Serializable> result = Collections.emptyMap();
//        if (CollectionUtils.isNotEmpty(entries)) {
//            Map<String, Serializable> resultMap = new HashMap<>(entries.size());
//            entries.stream().filter(Objects::nonNull)
//                .forEach(dataEntry ->
//                    resultMap.put(ObjectUtils.defaultIfNull(dataEntry.getKey(), StringUtils.EMPTY)
//                        .toString(), (Serializable)dataEntry.getValue())
//                );
//            result = resultMap;
//        }
//        return result;
//    }
//
//    @Override
//    public Map<String, Map<String, Serializable>> getDataWithMutilPkeySkey(ProcessorContext context, LinkedHashMap<String, List<String>> pkeySkeyMap) {
//        Result<Map<Object, Map<Object, Result<DataEntry>>>> result = TairPkeySkeyParser.from(mcTairManager, nameSpace, pkeySkeyMap)
//            .retrieveMap();
//        Map<Object, Map<Object, Result<DataEntry>>> data = result == null ? Collections.emptyMap() : MapUtils.emptyIfNull(result
//            .getValue());
//        Map<String, Map<String, Serializable>> stringMapMap = new HashMap<>(data.size());
//        for (Object key : data.keySet()) {
//            if (Objects.nonNull(key)) {
//                Map<Object, Result<DataEntry>> val = data.get(key);
//                if (MapUtils.isEmpty(val)) {
//                    stringMapMap.put(key.toString(), Collections.emptyMap());
//                } else {
//                    Map<String, Serializable> map = new HashMap<>(val.size());
//                    for (Object key1 : val.keySet()) {
//                        if (Objects.nonNull(key1)) {
//                            Result<DataEntry> dataEntryResult = val.get(key1);
//                            map.put(key1.toString(), dataEntryResult == null || dataEntryResult.getValue() == null ? null : (Serializable)dataEntryResult
//                                .getValue().getValue());
//                        }
//                    }
//                    stringMapMap.put(key.toString(), map);
//                }
//            }
//        }
//        Map<String, Map<String, Serializable>> tmp = new HashMap<>(pkeySkeyMap.size());
//        for (Map.Entry<String, List<String>> entry : pkeySkeyMap.entrySet()) {
//            Map<String, Serializable> value = new HashMap<>(stringMapMap.getOrDefault(entry.getKey(), Collections.emptyMap()));
//            Map<String, Serializable> map = new HashMap<>(entry.getValue().size());
//            for (String str : entry.getValue()) {
//                map.put(str, value.get(str));
//            }
//            tmp.put(entry.getKey(), map);
//        }
//        return tmp;
//    }
//
//    @Override
//    public boolean putDatas(String pkey, Map<Object, Object> skeys) {
//        List<KeyValuePack> list = skeys.entrySet().stream().map(e -> {
//            KeyValuePack keyValuePack = new KeyValuePack();
//            keyValuePack.setKey(e.getKey());
//            keyValuePack.setValue(e.getValue());
//            return keyValuePack;
//        }).collect(Collectors.toList());
//        Result<Map<Object, ResultCode>> result = mcTairManager.prefixPuts(nameSpace, pkey, list);
//        return result.isSuccess();
//    }
//
//    /**
//     * get key data from ldb
//     *
//     * @param key
//     * @return
//     */
//    @Override
//    public Serializable getData(ProcessorContext context, String key) {
//        Serializable result = null;
//        DataEntry entry = getDataReturnDataEntry(context, key);
//        if (entry != null && entry.getValue() != null) {
//            result = (Serializable)entry.getValue();
//        }
//        return result;
//    }
//
//    @Override
//    public Serializable getData(ProcessorContext context, String key, int expire) {
//        if (expire == -1) {
//            return getData(context, key);
//        }
//        Serializable result = null;
//        DataEntry entry = getDataReturnDataEntry(context, key);
//        if (entry != null && entry.getValue() != null) {
//            if ((System.currentTimeMillis() / 1000 - entry.getModifyDate()) <= expire) {
//                result = (Serializable)entry.getValue();
//            }
//        }
//        return result;
//    }
//
//    @Override
//    public Serializable getDataWithPkeySkey(ProcessorContext context, String pkey, String skey) {
//        Serializable result = null;
//        DataEntry entry = getDataWithPKeySkey(context, pkey, skey);
//        if (entry != null && entry.getValue() != null) {
//            result = (Serializable)entry.getValue();
//        }
//        return result;
//    }
//
//    private DataEntry getDataWithPKeySkey(ProcessorContext context, String pkey, String skey) {
//        if (StringUtils.isBlank(pkey)) {
//            Logger.onlineWarn(getErrorMessage(pkey, skey, null, "Tair getDataWithPKeySkey key is empty"));
//            return null;
//        }
//        try {
//            if (!CommonMethods.NAME_SPACE_WHITE_LIST.contains(nameSpace)) {
//                pkey = CommonMethods.parseCacheKey(pkey);
//            }
//            long start = System.currentTimeMillis();
//            Result<DataEntry> dataEntryResult = TairParserFactory.getParser(mcTairManager, nameSpace, pkey, skey)
//                .retrieve();
//
//
//            long cost = System.currentTimeMillis() - start;
//            writeMetrics2Counter(cost);
//            if (!dataEntryResult.isSuccess()) {
//                COUNTER.countSum(TppCounterNames.TAIR_GET_FAIL.getCounterName() + "_" + nameSpace, 1);
//            }
//            if (Objects.nonNull(dataEntryResult) && dataEntryResult.isSuccess() && Objects.nonNull(dataEntryResult.getValue())) {
//                if (Objects.nonNull(context)) {
//                    context.getContextData()
//                        .put(ConstantsFrame.LDB_LOCAL_CONTEXT_DATA_VERSION + nameSpace + pkey, dataEntryResult.getValue()
//                            .getVersion());
//                }
//                return dataEntryResult.getValue();
//            }
//        } catch (Exception e) {
//            COUNTER.countSum(TppCounterNames.TAIR_GET_EXCEPTION.getCounterName() + "_" + nameSpace, 1);
//            Logger.onlineWarn(getErrorMessage(pkey, skey, null, "Tair getDataWithPKeySkey error", e));
//        }
//        return null;
//    }
//
//    private DataEntry getDataReturnDataEntry(ProcessorContext context, String key) {
//        return getDataWithPKeySkey(context, key, "");
//    }
//
//    private List<DataEntry> getDataReturnDataEntry(ProcessorContext context, List<String> pkeys) {
//        if (CollectionUtils.isEmpty(pkeys)) {
//            Logger.onlineWarn(getErrorMessage(pkeys.toString(), "Tair getDataReturnDataEntry keys is empty"));
//            return null;
//        }
//        Result<List<DataEntry>> dataEntryResult = null;
//        try {
//            if (!CommonMethods.NAME_SPACE_WHITE_LIST.contains(nameSpace)) {
//                pkeys = CommonMethods.parseCacheKeyList(pkeys);
//            }
//            long start = System.currentTimeMillis();
//            dataEntryResult = mcTairManager.mget(nameSpace, pkeys);
//            long cost = System.currentTimeMillis() - start;
//            writeMetrics2Counter(cost);
//            if (!dataEntryResult.isSuccess()) {
//                COUNTER.countSum(TppCounterNames.TAIR_GET_FAIL.getCounterName() + "_" + nameSpace, 1);
//            }
//            if (Objects.nonNull(dataEntryResult) && dataEntryResult.isSuccess() && dataEntryResult.getValue() != null) {
//                if (Objects.nonNull(context)) {
//                    for (DataEntry entry : dataEntryResult.getValue()) {
//                        context.getContextData()
//                            .put(ConstantsFrame.LDB_LOCAL_CONTEXT_DATA_VERSION + nameSpace + entry.getKey(), entry.getVersion());
//                    }
//                }
//                return dataEntryResult.getValue();
//            } else {
//                return null;
//            }
//        } catch (Exception e) {
//            COUNTER.countSum(TppCounterNames.TAIR_GET_EXCEPTION.getCounterName() + "_" + nameSpace, 1);
//            Logger.onlineWarn(getErrorMessage(pkeys.toString(), null,
//                (Objects.nonNull(dataEntryResult) ? dataEntryResult.getRc() : null), "Tair getDataReturnDataEntry pkeys error", e));
//        }
//        return null;
//    }
//    @Override
//    public Integer incr(String key, int value, int defaultValue, int expireTime) {
//        if (StringUtils.isBlank(key)) {
//            Logger.onlineWarn(getErrorMessage(key, "Tair incr key is empty"));
//            return null;
//        }
//        try {
//            key = CommonMethods.parseCacheKey(key);
//            long start = System.currentTimeMillis();
//            Result<Integer> result = mcTairManager.incr(nameSpace, key, value, defaultValue, expireTime);
//            long cost = System.currentTimeMillis() - start;
//            COUNTER.countAvg(TppCounterNames.TAIR_INCR_RT.getCounterName() + "_" + nameSpace, cost);
//            COUNTER.countSum(TppCounterNames.TAIR_INCR_QPS.getCounterName() + "_" + nameSpace, 1);
//            if (result.isSuccess() && result.getValue() != null) {
//                return result.getValue();
//            }
//        } catch (Exception e) {
//            COUNTER.countSum(TppCounterNames.TAIR_INCR_EXCEPTION.getCounterName() + "_" + nameSpace, 1);
//            Logger.onlineWarn(getErrorMessage(key, null, null, "Tair incr error", e));
//        }
//        return null;
//    }
//
//    /**
//     * @param key
//     * @param value
//     * @param expireTime
//     * @return
//     */
//    @Override
//    public boolean putData(String key, Serializable value, int expireTime) {
//        if (expireTime != -1) {
//            return putData(key, value, 0, expireTime);
//        } else {
//            return putData(key, value);
//        }
//    }
//
//    /**
//     * @param key
//     * @param value
//     * @return
//     */
//    @Override
//    public boolean putData(String key, Serializable value) {
//        return putData(key, value, 0, 0);
//    }
//
//    /**
//     * @param key
//     * @param value
//     * @param version
//     * @param expireTime
//     * @return
//     */
//    @Override
//    public boolean putData(String key, Serializable value, int version, int expireTime) {
//        ResultCode code = putDataReturnResultCode(key, value, version, expireTime);
//        return code.isSuccess();
//    }
//
//    /**
//     * @param key
//     * @param value
//     * @param expireTime
//     * @return
//     */
//    @Override
//    public boolean putDataCheckVersion(ProcessorContext context, String key, Serializable value, int expireTime,
//                                       ILdbDataMerge merge) {
//        Integer version = getOriginVersion(context, key);
//        ResultCode code = putDataReturnResultCode(key, value, version, expireTime);
//        int tryTimeConfig = getSaveRetryTimeConfig();
//        int tryTime = tryTimeConfig;
//        while (code.getCode() != 0 && code.getCode() != ResultCode.KEYORVALUEISNULL.getCode() && tryTime > 0) {
//            tryTime--;
//            if (code.getCode() == ResultCode.VERERROR.getCode()) {
//                writeVersionErrorCounter();
//                String data = null;
//                DataEntry entry = getDataReturnDataEntry(context, key);
//                if (Objects.nonNull(entry) && Objects.nonNull(entry.getValue())) {
//                    data = entry.getValue().toString();
//                    version = entry.getVersion();
//                }
//                value = merge.merge(data, value);
//            } else {
//                writeOtherErrorCounter(code);
//            }
//            code = putDataReturnResultCode(key, value, version, expireTime);
//        }
//        writeRetryTimesCounter(tryTimeConfig - tryTime);
//        if (code.getCode() == ResultCode.VERERROR.getCode() || code.getCode() != 0) {
//            writeForceOverwriteCounter();
//            code = putDataReturnResultCode(key, value, 0, expireTime);
//        }
//        if (code.getCode() == ResultCode.VERERROR.getCode() || code.getCode() != 0) {
//            Logger.onlineWarn(getErrorMessage(key, null, code, "Final Tair put error, old version:" + version));
//        }
//        return code.isSuccess();
//    }
//
//    private Integer getOriginVersion(ProcessorContext context, String key) {
//        Integer version = 0;
//        if (Objects.nonNull(context)) {
//            version = context.getContextData(ConstantsFrame.LDB_LOCAL_CONTEXT_DATA_VERSION + nameSpace + key);
//            final Integer originVersion = version;
//            Debugger.put(this, () -> "TairVersion", () -> Objects.isNull(originVersion) ? "null" : originVersion);
//            Debugger.put(this, () -> "TairVersionContextKey", () -> nameSpace + key);
//            // https://yuque.antfin-inc.com/tair-userdoc/ldb/bcbaer
//            // first time write version set to 2
//            if (version == null) {
//                version = 2;
//            }
//        }
//        return version;
//    }
//
//    /**
//     * write data with version check, if version check error, return immediately
//     * @param context
//     * @param key
//     * @param value
//     * @param expireTime
//     * @return
//     */
//    @Override
//    public ResultCode putDataWithVersionCheck(ProcessorContext context, String key, Serializable value, int expireTime) {
//        Integer version = getOriginVersion(context, key);
//        ResultCode code = putDataReturnResultCode(key, value, version, expireTime);
//        int tryTimeConfig = getSaveRetryTimeConfig();
//        int tryTime = tryTimeConfig;
//        while (code.getCode() != 0 && code.getCode() != ResultCode.KEYORVALUEISNULL.getCode() && tryTime > 0) {
//            tryTime--;
//            if (code.getCode() == ResultCode.VERERROR.getCode()) {
//                writeVersionErrorCounter();
//                return code;
//            } else {
//                writeOtherErrorCounter(code);
//            }
//            code = putDataReturnResultCode(key, value, version, expireTime);
//        }
//        if (tryTimeConfig != tryTime) {
//            writeRetryTimesCounter(tryTimeConfig - tryTime);
//        }
//        if (code.getCode() == ResultCode.VERERROR.getCode() || code.getCode() != 0) {
//            writeForceOverwriteCounter();
//            code = putDataReturnResultCode(key, value, 0, expireTime);
//        }
//        return code;
//    }
//
//    private ResultCode putDataReturnResultCode(String key, Serializable value, int version, int expireTime) {
//        boolean keyValueNull = StringUtils.isEmpty(key) || Objects.isNull(value);
//        boolean valueStringEmpty = value instanceof String && StringUtils.isEmpty((String)value);
//        if (keyValueNull || valueStringEmpty) {
//            Logger.onlineWarn(getErrorMessage(key, "Tair put key or value is empty,value:" + value));
//            return ResultCode.valueOf(ResultCode.KEYORVALUEISNULL.getCode());
//        }
//        ResultCode code;
//        try {
//            key = CommonMethods.parseCacheKey(key);
//            long start = System.currentTimeMillis();
//            code = mcTairManager.put(nameSpace, key, value, version, expireTime);
//            long cost = System.currentTimeMillis() - start;
//            COUNTER.countSum(TppCounterNames.TAIR_PUT_QPS.getCounterName() + "_" + nameSpace, 1);
//            COUNTER.countAvg(TppCounterNames.TAIR_PUT_RT.getCounterName() + "_" + nameSpace, cost);
//        } catch (Exception e) {
//            COUNTER.countSum(TppCounterNames.TAIR_PUT_EXCEPTION.getCounterName() + "_" + nameSpace, 1);
//            Logger.onlineWarn(getErrorMessage(key, null, null, "Tair putDataReturnResultCode error", e));
//            return ResultCode.valueOf(ResultCode.UNKNOW.getCode());
//        }
//        return code;
//    }
//
//    private Integer getSaveRetryTimeConfig() {
//        return Integer.parseInt(ServiceFactory.getTppConfigService().getString(TAIR_SAVE_RETRY_TIME_KEY, TAIR_SAVE_RETRY_TIME_DEFAULT));
//    }
//
//    @Override
//    public String toString() {
//        return "[userName:" + userName
//            + ",namespace:"
//            + nameSpace
//            + ",timeout:"
//            + timeout
//            + ",unit:"
//            + unit + "]";
//    }
//
//    private String getErrorMessage(String message) {
//        return getErrorMessage(null, null, null, message, null);
//    }
//
//    private String getErrorMessage(String pKey, String message) {
//        return getErrorMessage(pKey, null, null, message, null);
//    }
//
//    private String getErrorMessage(String pKey, ResultCode code, String message) {
//        return getErrorMessage(pKey, null, code, message, null);
//    }
//
//    private String getErrorMessage(String pKey, String sKey, ResultCode code, String message) {
//        return getErrorMessage(pKey, sKey, code, message, null);
//    }
//
//    private String getErrorMessage(String pKey, String sKey, ResultCode code, String message, Exception e) {
//        StringBuilder msg = new StringBuilder(this.toString());
//        if (Objects.nonNull(pKey)) {
//            msg.append(",pKey:").append(pKey);
//        }
//        if (Objects.nonNull(sKey)) {
//            msg.append(",sKey:").append(sKey);
//        }
//        if (Objects.nonNull(code)) {
//            msg.append(",code:").append(code.getCode());
//        }
//        if (Objects.nonNull(message)) {
//            msg.append(",msg:").append(message);
//        }
//        if (Objects.nonNull(e)) {
//            msg.append(",exception:").append(e.getMessage());
//        }
//        if (Objects.nonNull(e)) {
//            msg.append(",stack:").append(ExceptionUtils.getStackTrace(e));
//        }
//        return msg.toString();
//    }
//
//    private void writeMetrics2Counter(long cost) {
//        COUNTER.countAvg(TppCounterNames.TAIR_GET_RT.getCounterName() + "_" + nameSpace, cost);
//        COUNTER.countSum(TppCounterNames.TAIR_GET_QPS.getCounterName() + "_" + nameSpace, 1);
//    }
//
//    private void writeOtherErrorCounter(ResultCode code) {
//        COUNTER.countSum(TppCounterNames.TAIR_PUT_OTHER_ERROR.getCounterName(), 1);
//        COUNTER.countSum(TppCounterNames.TAIR_PUT_OTHER_ERROR.getCounterName() + "_" + nameSpace, 1);
//        COUNTER.countSum(TppCounterNames.TAIR_PUT_ERROR_CODE.getCounterName() + nameSpace + "_" + code.getCode(), 1);
//    }
//
//    private void writeVersionErrorCounter() {
//        COUNTER.countSum(TppCounterNames.TAIR_VERSION_ERROR.getCounterName(), 1);
//        COUNTER.countSum(TppCounterNames.TAIR_VERSION_ERROR.getCounterName() + "_" + nameSpace, 1);
//    }
//
//    private void writeForceOverwriteCounter() {
//        COUNTER.countSum(TppCounterNames.TAIR_FORCE_OVERWRITE.getCounterName(), 1);
//        COUNTER.countSum(TppCounterNames.TAIR_FORCE_OVERWRITE.getCounterName() + "_" + nameSpace, 1);
//    }
//
//    private void writeRetryTimesCounter(int times) {
//        COUNTER.countAvg(TppCounterNames.TAIR_RETRY_TIME.getCounterName() + "_" + nameSpace, times);
//    }
//
//}
