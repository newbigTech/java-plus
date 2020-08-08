package org.java.plus.dag.core.base.utils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.util.TypeUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
//import com.taobao.recommendplatform.protocol.solution.Context;
//import com.taobao.recommendplatform.protocol.solution.TppHyperspaceResult;
import org.java.plus.dag.core.base.annotation.ManualConfig;
import org.java.plus.dag.core.base.annotation.ManualParam;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.constants.TppCounterNames;
import org.java.plus.dag.core.base.exception.StatusType;
import org.java.plus.dag.core.base.model.ConfigKey;
import org.java.plus.dag.core.base.model.ManualFieldType;
import org.java.plus.dag.core.base.proc.Processor;
import org.java.plus.dag.core.base.utils.tair.TairClient;
import org.java.plus.dag.solution.Context;
//import org.java.plus.dag.core.base.utils.tair.TairUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

/**
 * Tpp/Hyperspace layer utils
 *
 * @author seven.wxy
 * @date 2019/5/22
 */
public class LayerUtils {

    public static boolean isEnableTppLayer() {
        return ThreadLocalUtils.isEnableTppLayer();
    }

    public static boolean isEnableManualLayer() {
        return ThreadLocalUtils.isEnableManualLayer();
    }

//    public static TppHyperspaceResult getTppHyperspaceResult() {
//        Context context = ThreadLocalUtils.getContext();
//        return ObjectUtils.defaultIfNull(context == null ? null : context.getHyperspaceResult(), null);
//    }

//    public static TppHyperspaceResult getManualHyperspaceResult() {
//        return ThreadLocalUtils.getManualHyperspaceResult();
//    }

    public static Map<String, JSONObject> getManualHyperspaceParamsResult() {
        return ThreadLocalUtils.getManualHyperspaceParamsResult();
    }

    public static Map<String, String> getTppHyperspaceParamsResult() {
        return ThreadLocalUtils.getTppHyperspaceParamsResult();
    }

    public static Map<String, JSONObject> getTppHyperspaceParamsJsonResult() {
        return ThreadLocalUtils.getTppHyperspaceParamsJsonResult();
    }

    /**
     * replace tpp layer config value before get tpp config
     *
     * @param key
     * @param defaultValue
     * @return
     */
    public static JSONObject getTppConfig(String key, JSONObject defaultValue) {
        Map<String, JSONObject> hitParams;
        if (isEnableTppLayer() && MapUtils.isNotEmpty(hitParams = getTppHyperspaceParamsJsonResult()) && hitParams.containsKey(key)) {
            JSONObject hitValue = hitParams.get(key);
            JSONObject result = new JSONObject(hitValue.size()).fluentPutAll(hitValue);
            Debugger.put(LayerUtils.class, () -> "tpp_layer_hit_key_result", () -> key);
            Debugger.put(LayerUtils.class, () -> "tpp_layer_hit_value_result", () -> result);
            return result;
        } else {
            return TppConfigUtil.getJson(key, defaultValue);
        }
    }

    /**
     * replace tpp layer config key before get tpp config
     *
     * @param configKey
     * @return
     */
    public static ConfigKey replaceTppLayerConfigKey(ConfigKey configKey) {
        Map<String, String> hitParams;
        if (isEnableTppLayer() && MapUtils.isNotEmpty(hitParams = getTppHyperspaceParamsResult())) {
            boolean hit = false;
            if (StringUtils.isNotEmpty(configKey.getConfigKeyPrefix())) {
                String prefix = hitParams.get(configKey.getConfigKeyPrefix());
                if (Objects.nonNull(prefix)) {
                    hit = true;
                    configKey.setConfigKeyPrefix(prefix);
                }
            } else {
                String key = hitParams.get(configKey.getConfigKey());
                if (Objects.nonNull(key)) {
                    hit = true;
                    configKey.setConfigKey(key);
                }
            }
            if (hit) {
                Debugger.put(LayerUtils.class, () -> "tpp_layer_hit", () -> configKey + "|" + hitParams);
            } else {
                // if use layer key and not hit, reset to origin key
                resetTppLayerKey(configKey);
            }
        } else {
            // if use layer key and not hit, reset to origin key
            resetTppLayerKey(configKey);
        }
        return configKey;
    }

    /**
     * replace manual layer config value after get tpp config
     *
     * @param configKey
     * @param configValue
     * @return
     */
    public static JSONObject replaceManualLayerConfigValue(ConfigKey configKey, JSONObject configValue) {
        Map<String, JSONObject> hitParams;
        JSONObject result = configValue;
        if (isEnableManualLayer() && MapUtils.isNotEmpty(hitParams = getManualHyperspaceParamsResult())) {
            String instanceKey = configKey.toString();
            JSONObject hitValues = hitParams.get(instanceKey);
            if (MapUtils.isNotEmpty(hitValues)) {
//                ServiceFactory.getTPPCounter().countSum(TppCounterNames.MANUAL_LAYER_HIT.getCounterName(), 1);
                Debugger.put(LayerUtils.class, () -> "manual_layer_hit", () -> configKey + "|" + hitParams);
                // configValue instance in tpp config cache, can't modify
                result = new JSONObject(configValue.size() + hitValues.size()).fluentPutAll(configValue)
                        .fluentPutAll(hitValues);
                // add layer info to instanceKey, new instanceKey will new instance use hit config
                Debugger.put(LayerUtils.class, () -> "manual_layer_hit_key_result", () -> configKey.toString());
                Debugger.put(LayerUtils.class, "manual_layer_hit_value_result", result);
            }
        }
        return result;
    }

    public static ConfigKey resetTppLayerKey(ConfigKey configKey) {
        String originConfigKey;
        if (Objects.nonNull(originConfigKey = TppObjectFactory.getLayerBeanKeyCache().get(configKey.getConfigKey()))) {
            configKey.setConfigKey(originConfigKey);
        }
        String originPrefix;
        if (Objects.nonNull(originPrefix = TppObjectFactory.getLayerBeanKeyCache()
                .get(configKey.getConfigKeyPrefix()))) {
            configKey.setConfigKeyPrefix(originPrefix);
        }
        return configKey;
    }

    public static void writeManualToTair(List<Field> allFieldList, Processor processor) {
        Map<String, Object> manualConfig = null;
        List<Map<String, Object>> manualParamList = Lists.newArrayList();
        try {
            for (Field field : allFieldList) {
                ManualConfig another = field.getAnnotation(ManualConfig.class);
                if (Objects.nonNull(another)) {
                    if (Objects.nonNull(manualConfig) || StringUtils.contains(another.name(), StringPool.UNDERSCORE)) {
                        throw new RuntimeException(
                                "Processor config should contain only one ManualConfig annotation,ManualConfig's name must not contain '_'");
                    }
                    String key = another.key();
                    key = StringUtils.isEmpty(key) ? String.valueOf(FieldUtils.readField(field, processor, true)) : key;
                    Map<String, Object> tmp = JsonUtils.annotationToMap(another);
                    tmp.put("key", key);
                    manualConfig = tmp;
                    continue;
                }
                ManualParam manualParam = field.getAnnotation(ManualParam.class);
                if (Objects.nonNull(manualParam)) {
                    String key = manualParam.key();
                    key = StringUtils.isEmpty(key) ? field.getName() : key;
                    Map<String, Object> map = formalizeManualParam(JsonUtils.annotationToMap(manualParam));
                    map.put("key", key);
                    manualParamList.add(map);
                }
            }
        } catch (Exception e) {
            Debugger.exception(LayerUtils.class, StatusType.PARAM_PARSE_EXCEPTION.getStatus(), "writeManualToTair exception", e);
            throw new RuntimeException(e);
        }
        if (CollectionUtils.isNotEmpty(manualParamList)) {
            Map<String, Object> instanceKeyMap = Maps.newHashMap();
            instanceKeyMap.put("key", ConstantsFrame.MANUAL_INSTANCE_KEY);
            instanceKeyMap.put("name", "INSTANCE_ID");
            instanceKeyMap.put("type", ManualFieldType.HIDDEN.name());
            instanceKeyMap.put("defaultValue", processor.getInstanceKey());
            instanceKeyMap.put("required", true);
            manualParamList.add(instanceKeyMap);
        }
        if (Objects.nonNull(manualConfig) && CollectionUtils.isNotEmpty(manualParamList)) {
            writeManualConfigToTair(manualConfig, manualParamList);
        }
    }

    private static void writeManualConfigToTair(Map<String, Object> manualConfig, List<Map<String, Object>> manualParamList) {
//        TairClient tairClient = TairUtil.getUnifiedTair();
        Context context = ThreadLocalUtils.getContext();
        if (Objects.nonNull(context)) {
            Map<Object, Object> map = new HashMap<>(1);
            Map<Object, Object> tmp = new HashMap<>(2);
            tmp.put("name", manualConfig.get("name"));
            tmp.put("parameters", manualParamList);
            map.put(manualConfig.get("type") + StringPool.UNDERSCORE + manualConfig.get("key"), tmp);
            String pKey = ConstantsFrame.MANUAL_CONFIG_TAIR_PREFIX + context.getCurrentAppId();
            Debugger.put(LayerUtils.class, () -> "manualConfigKey", () -> pKey);
            Debugger.put(LayerUtils.class, () -> "manualConfigValue", () -> map);
//            tairClient.putDatas(pKey, map);
        }
    }

    private static Map<String, Object> formalizeManualParam(Map<String, Object> manualParam) {
        manualParam.put("parameters", _doFormalized(manualParam.get("parameters")));
        manualParam.put("range", _doFormalized(manualParam.get("range")));
        try {
            Object defaultValue = TypeUtils.castToString(manualParam.get("defaultValue"));
            manualParam.put("defaultValue", defaultValue);
        } catch (Exception e) {
            //ignore
        }
        return manualParam;
    }

    private static JSONArray _doFormalized(Object obj) {
        String objStr = TypeUtils.castToString(obj);
        if (StringUtils.isEmpty(objStr)) {
            return ConstantsFrame.NULL_JSON_ARRAY;
        }
        Object result = null;
        try {
            result = JSON.toJSON(obj);
        } catch (Exception e) {
            //ignore
        }
        if (result instanceof JSONArray) {
            JSONArray jsonArray = new JSONArray();
            JSONArray array = (JSONArray) result;
            for (Object var : array) {
                if (var instanceof String) {
                    String str = TypeUtils.castToString(var);
                    if (StringUtils.isEmpty(str)) {
                        jsonArray.add(ConstantsFrame.NULL_JSON_ARRAY);
                    } else {
                        try {
                            jsonArray.addAll(JSONArray.parseArray(str));
                        } catch (Exception e) {
                            jsonArray.add(JSONObject.parseObject(str));
                        }
                    }
                }
            }
            return jsonArray;
        } else if (result instanceof String) {
            try {
                return JSONArray.parseArray((String) result);
            } catch (Exception e) {
                return ConstantsFrame.NULL_JSON_ARRAY;
            }
        }
        return new JSONArray().fluentAdd(result);
    }
}