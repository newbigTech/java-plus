package org.java.plus.dag.core.base.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

import com.google.common.collect.Maps;
//import com.taobao.recommendplatform.protocol.domain.abfs.AbfsNewResult;
//import com.taobao.recommendplatform.protocol.domain.abfs.AbfsPersonalizerResult;
//import com.taobao.recommendplatform.protocol.domain.abfs.AbfsRequest;
//import com.taobao.recommendplatform.protocol.domain.abfs.AbfsRequestBuilder;
//import com.taobao.recommendplatform.protocol.service.AbfsNewService;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
import org.java.plus.dag.core.base.em.FieldNameEnum;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.taobao.AbfsPersonalizerResult;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @Author: zoe.xdf
 * @Description: get features from abfs by view name
 * @Date: 2019/9/17
 */
@Data
@Accessors(fluent = true)
public class DbsRequestBuilder {
//    private static final AbfsNewService DBS_SERVICE = ServiceFactory.getAbfsNewService();

    public static final String DEFAULT_VIEW_NAME = StringUtils.EMPTY;
    public static final int DEFAULT_TIMEOUT_MS = 100;
    public static final String DEFAULT_VIP_SERVER = "com.ykrec.unified_turing_service";
    public static final String DEFAULT_OUTFMT = "fb2";
    public static final String DEFAULT_REQ_PREFIX = "be";
    public static final String DEFAULT_TURING_SERVICE = "unified_service";
    public static final String ABFS_PERSONALIZER = "abfs_personalizer";

    private String viewName = DEFAULT_VIEW_NAME;
    private int timeoutMs = DEFAULT_TIMEOUT_MS;
    private String vipServer = DEFAULT_VIP_SERVER;
    private String outFmt = DEFAULT_OUTFMT;
    private String reqPrefix = DEFAULT_REQ_PREFIX;
    private String turingService = DEFAULT_TURING_SERVICE;
    private String pvId = StringUtils.EMPTY;
    private Map<String, String> params = new HashMap<>();

    private DbsRequestBuilder() {}

    public static DbsRequestBuilder prepareRequest() { return new DbsRequestBuilder(); }

    public DbsRequestBuilder addParam(String key, String val) {
        this.params.put(key, val);
        return this;
    }

    public DbsRequestBuilder addParam(Map<String, String> params) {
        if (null == params || params.isEmpty()){
            return this;
        }
        this.params.putAll(params);
        return this;
    }

//    private AbfsRequest buildAbfsRequest(){
//        AbfsRequest abfsRequest = new AbfsRequestBuilder()
//            .setPath(reqPrefix)
//            .setFgConfig(turingService)
//            .addFeatures(viewName)
//            .setFormat(outFmt)
//            .setPvid(pvId)
//            .addParams(params)
//            .build();
//        abfsRequest.setVipserverDomain(vipServer);
//        abfsRequest.setTimeout(timeoutMs);
//        return abfsRequest;
//    }

    public Map<String, List<JSONObject>> sendRequest() {
//        AbfsRequest abfsRequest = buildAbfsRequest();
        try {
//            AbfsNewResult abfsResult = DBS_SERVICE.query(abfsRequest);
//            if (abfsResult != null) {
//                AbfsPersonalizerResult abfsPersonalizerResult =
//                    abfsResult.getPersonalizerResults().get(ABFS_PERSONALIZER);
//                if (abfsPersonalizerResult != null) {
//                    return parseFB2(abfsPersonalizerResult);
//                }
//            }
        } catch (Exception e) {
            Logger.error(e::getMessage, e);
        }
        return new HashMap<>();
    }

    private Map<String, List<JSONObject>> parseFB2(AbfsPersonalizerResult abfsResult) {
//        Map<String, List<JSONObject>> result = new HashMap<>(abfsResult.getFeatures().size());
//        abfsResult.getFeatures().forEach((featureName, table) -> {
//            if(table != null && table.getValueLines() > 0) {
//                List<JSONObject> featureVal = new ArrayList<>(table.getValueLines());
//                List<String> fieldNames = table.getFieldNames();
//                for (int i = 0; i < table.getValueLines(); i++) {
//                    JSONObject cur = new JSONObject();
//                    for(String field : fieldNames) {
//                        String value = table.getString(i, field);
//                        cur.put(field,value);
//                    }
//                    featureVal.add(cur);
//                }
//                result.put(featureName, featureVal);
//            }
//        });
//        return result;
    	return null;
    }

    public static Map<String, DataSet<Row>> parseResult(Map<String, List<JSONObject>> result, String keyPrefix) {
        Map<String, DataSet<Row>> parseResult = Maps.newHashMap();
        if (MapUtils.isNotEmpty(result)) {
            result.forEach((key, value) -> parseResult.put(keyPrefix + key, parseResult(key, value)));
        }
        return parseResult;
    }

    public static DataSet<Row> parseResult(String viewName, List<JSONObject> result) {
        DataSet<Row> dataSet = new DataSet<>();
        if (CollectionUtils.isNotEmpty(result)) {
            result.forEach(e -> {
                Map<FieldNameEnum, Object> returnMap = Maps.newHashMapWithExpectedSize(e.size());
                e.forEach((k, v) -> returnMap.put(EnumUtil.getEnum(k), v));
                dataSet.getData().add(new Row(returnMap));
            });
        }
        dataSet.setSource(viewName);
        return dataSet;
    }
}
