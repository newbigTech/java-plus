package org.java.plus.dag.core.ds.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.alibaba.fastjson.JSONObject;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.model.ParameterConfig;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.utils.JsonUtils;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author seven.wxy
 * @date 2018/9/25
 */
@Data
public class BEDataSourceConfig {
    private int returnCount;
    private String vipServer;
    private String bizName;
    private String searcher;
    private String proxyer;
    private String outfmt;
    private int timeout;
    private boolean async;
    private Map<String, Object> optionalParam;
    private Map<String, String> aliasMap;
    private List<String> responseValueField;

    public static final String REQUEST_OPTIONAL_KEY = "optional";
    public static final String ALIAS = "alias";
    public static final String REQUEST_REQUIRED_KEY = "required";
    public static final String REQUEST_ASYNC = "async";
    public static final String RESPONSE_VALUE_FIELD = "response_value_field";
    public static final Splitter COMMA_SPLITTER = Splitter.on(",");

    public enum BEParameter implements ParameterConfig {

        BE_RETURN_COUNT("returnCount", 1000, "be item return count"),
        BE_VIP_SERVER("vipServer", "com.taobao.search.ykrec_basic_engine.vipserver", "be item return count"),
        BE_BIZ_NAME("bizName", "ykrec_be_searcher", "be business name"),
        BE_SEARCHER("searcher", "baseline", "be searcher"),
        BE_PROXYER("proxyer", "merge", "be proxyer"),
        BE_OUTFMT("outfmt", "json", "search primary key"),
        BE_TIMEOUT("timeout", 80, "be timeout"),
        BE_REQUEST_PARAM(REQUEST_OPTIONAL_KEY, new JSONObject(), "request extend param"),
        BE_RESPONSE_VALUE_FIELD(RESPONSE_VALUE_FIELD, "",
                "the field name we fetch from be match record. eq: field1,field2, "
                        + "then we only fetch field1, field2"),
        BE_ASYNC("async", false, "be request async or not");

        BEParameter(String name, Object defaultValue, String desc) {
            this.name = name;
            this.defaultValue = defaultValue;
            this.desc = desc;
        }

        @Getter
        private String name;
        @Getter
        private Object defaultValue;
        @Getter
        private String desc;
    }

    public void addRequestParam(String key, String value) {
        if (Objects.isNull(optionalParam)) {
            optionalParam = new HashMap<>();
        }
        optionalParam.put(key, value);
    }

    public void addRequestParamMap(Map<String, String> paramMap) {
        if (Objects.isNull(optionalParam)) {
            optionalParam = Maps.newHashMap();
        }
        optionalParam.putAll(paramMap);
    }

    public BEDataSourceConfig(int returnCount, String vipServer, String bizName, String searcher,
                              String proxyer, String outfmt, int timeout, boolean async,
                              Map<String, Object> optionalParam, List<String> responseValueField,
                              Map<String, String> alMap) {
        this.returnCount = returnCount;
        this.vipServer = vipServer;
        this.bizName = bizName;
        this.searcher = searcher;
        this.proxyer = proxyer;
        this.outfmt = outfmt;
        this.timeout = timeout;
        this.optionalParam = optionalParam;
        this.responseValueField = responseValueField;
        this.async = async;
        this.aliasMap = alMap;
    }

    public static BEDataSourceConfig from(ProcessorConfig processorConfig) throws IllegalArgumentException {
        return from(processorConfig, null);
    }

    public static BEDataSourceConfig from(ProcessorConfig processorConfig, Map<String, String> paramMap)
            throws IllegalArgumentException {
        try {
            if (MapUtils.isEmpty(paramMap)) {
                paramMap = Maps.newHashMap();
            }
            Objects.requireNonNull(processorConfig);
            JSONObject requiredObj = JsonUtils.getParam(processorConfig, REQUEST_REQUIRED_KEY, new JSONObject());
            int returnCount = JsonUtils.getParam(requiredObj, BEParameter.BE_RETURN_COUNT);
            String vipServer = JsonUtils.getParam(requiredObj, BEParameter.BE_VIP_SERVER);
            String bizName = JsonUtils.getParam(requiredObj, BEParameter.BE_BIZ_NAME);
            String searcher = JsonUtils.getParam(requiredObj, BEParameter.BE_SEARCHER);
            String proxyer = JsonUtils.getParam(requiredObj, BEParameter.BE_PROXYER);
            String outfmt = JsonUtils.getParam(requiredObj, BEParameter.BE_OUTFMT);
            int timeout = JsonUtils.getParam(requiredObj, BEParameter.BE_TIMEOUT);
            boolean asyncParam = JsonUtils.getParam(requiredObj, BEParameter.BE_ASYNC);
            JSONObject optionalMap = JsonUtils.getParam(processorConfig, REQUEST_OPTIONAL_KEY, new JSONObject());
            Map<String, String> aliMap = new HashMap<>();
            if (MapUtils.isNotEmpty(optionalMap)) {
                optionalMap.putAll(paramMap);
                if (optionalMap.containsKey(ALIAS)) {
                    String alias = optionalMap.getString(ALIAS);
                    if (StringUtils.isNotBlank(alias)) {
                        String[] aliasPair = StringUtils.splitByWholeSeparator(alias, StringPool.COMMA);
                        for (String one : aliasPair) {
                            String[] onePair = StringUtils.splitByWholeSeparator(one, StringPool.EQUALS);
                            if (onePair.length > 1) {
                                aliMap.put(onePair[0], onePair[1]);
                            }
                        }
                    }
                }
            }
            String responseFieldStr = JsonUtils.getParam(processorConfig, BEParameter.BE_RESPONSE_VALUE_FIELD);
            List<String> responseValueField = COMMA_SPLITTER.splitToList(responseFieldStr);

            return new BEDataSourceConfig(returnCount, vipServer, bizName, searcher, proxyer, outfmt, timeout,
                    asyncParam, optionalMap, responseValueField, aliMap);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

}
