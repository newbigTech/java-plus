package org.java.plus.dag.core.ds.model;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

import com.google.common.collect.Maps;
import org.java.plus.dag.core.ds.utils.PlaceHolderUtil;
import lombok.Data;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: PlaceHolderDO
 * @Package org.java.plus.dag.frame.ds.model
 * @date 2018/11/14 上午11:46
 */
@Data
public class PlaceHolderDO {
    /**
     * 存放json路径 和 占位符
     */
    private Map<String, String> pathHolderMap;
    /**
     * 静态替换后的Map
     */
    private Map<String, String> doneMap;
    /**
     * 默认Map
     */
    private Map<String, Object> defaultMap;
    /**
     * 记录中间结果 替换后的Map
     */
    private Map<String, Object> replacedMap;

    public static final String TIMESTAMP_HOLDER = "timestamp";
    private static final String DEFAULT_PREFIX = "default";

    private static final String PKEY_STR = "pkey";
    private static final String SKEY_STR = "skey";

    public boolean isEmpty() {
        return MapUtils.isEmpty(pathHolderMap) && MapUtils.isEmpty(doneMap);
    }

    private PlaceHolderDO(Map<String, String> pathHolderMap,
                          Map<String, String> doneMap,
                          Map<String, Object> defaultMap,
                          Map<String, Object> replacedMap) {
        this.pathHolderMap = pathHolderMap;
        this.doneMap = doneMap;
        this.defaultMap = defaultMap;
        this.replacedMap = replacedMap;
    }

    public static PlaceHolderDO from(Object jsonObj) {
        //解析json 拿到占位符Map
        Map<String, String> pathValueMap = PlaceHolderUtil.getJsonPathMap(jsonObj);
        //替换固定占位符
        if (MapUtils.isEmpty(pathValueMap)) {
            return emptyHolder();
        }

        Map<String, Object> defaultMap = new HashMap<>();
        if (jsonObj instanceof JSONObject) {
            JSONObject jsonInput = (JSONObject)jsonObj;
            defaultMap = getDefaultMap(jsonInput);
        }

        Map<String, String> doneMap = new HashMap<>();
        pathValueMap.forEach((k, v) -> {
            if (v.contains(TIMESTAMP_HOLDER)) {
                doneMap.put(k, v);
            }
        });

        if (MapUtils.isNotEmpty(pathValueMap)) {
            return new PlaceHolderDO(pathValueMap, doneMap, defaultMap, Maps.newHashMap());
        }
        return emptyHolder();
    }

    private static Map<String, Object> getDefaultMap(JSONObject jsonObject) {
        return (JSONObject)jsonObject.getOrDefault(DEFAULT_PREFIX, new JSONObject());
    }

    public static PlaceHolderDO emptyHolder() {
        return new PlaceHolderDO(Maps.newHashMap(), Maps.newHashMap(), Maps.newHashMap(), Maps.newHashMap());
    }

    public boolean needReplacePKeyOrSKey() {
        if (MapUtils.isEmpty(replacedMap)) {
            return false;
        }
        return replacedMap.entrySet().stream().anyMatch(
            e -> e.getKey().contains(PKEY_STR) || e.getKey().contains(SKEY_STR));
    }

    public String getReplacePKey() {
        for (Map.Entry<String, Object> entry : replacedMap.entrySet()) {
            if (entry.getKey().contains(PKEY_STR)) {
                return (String)entry.getValue();
            }
        }
        return StringUtils.EMPTY;
    }
}
