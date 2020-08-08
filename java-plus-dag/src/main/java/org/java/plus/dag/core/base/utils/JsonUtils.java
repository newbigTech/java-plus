package org.java.plus.dag.core.base.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.java.plus.dag.core.base.model.ParameterConfig;
import org.apache.commons.lang3.StringUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.java.plus.dag.core.base.constants.ConstantsFrame.NULL_JSON_ARRAY;
import static org.java.plus.dag.core.base.constants.ConstantsFrame.NULL_JSON_OBJECT;


/**
 * @author seven.wxy
 * @date 2018/10/10
 */
@SuppressWarnings("unchecked")
public class JsonUtils {
    public static <T> T getParam(JSONObject json, String key, T defaultValue) {
        if (Objects.nonNull(json)) {
            if (defaultValue == null) {
                return (T) json.get(key);
            } else {
                T t = null;
                try {
                    t = json.getObject(key, (Class<T>) defaultValue.getClass());
                } catch (Exception e) {
                    //ignore
                }
                if (Objects.nonNull(t)) {
                    return t;
                }
            }
        }
        return defaultValue;
    }

    public static <T> T getParam(JSONObject json, ParameterConfig config) {
        if (Objects.nonNull(json)) {
            if (config.getDefaultValue() == null) {
                return (T) json.get(config.getName());
            } else {
                T t = null;
                try {
                    t = json.getObject(config.getName(), (Class<T>) config.getDefaultValue().getClass());
                } catch (Exception ignore) {
                }
                if (Objects.nonNull(t)) {
                    return t;
                }
            }
        }
        return config.getDefaultValue();
    }

    public static JSONArray getJsonArray(String jsonArray) {
        if (StringUtils.isNoneBlank(jsonArray)) {
            try {
                return JSONArray.parseArray(jsonArray);
            } catch (Exception e) {
                Logger.error("JSONArray parse error " + jsonArray, e);
            }
        }
        return NULL_JSON_ARRAY;
    }

    public static JSONObject getJsonObject(String json) {
        try {
            if (StringUtils.isNotBlank(json)) {
                return JSONObject.parseObject(json);
            }
        } catch (Exception e) {
            Logger.error("Json parse error " + json, e);
        }
        return NULL_JSON_OBJECT;
    }

    public static Map<String, Object> annotationToMap(Annotation annotation) {
        Map<String, Method> methodMap = sun.reflect.annotation.AnnotationType.getInstance(annotation.annotationType())
            .members();
        Map<String, Object> result = new HashMap<>(methodMap.size());
        methodMap.forEach((k, v) -> {
            try {
                if (v.getReturnType().isEnum()) {
                    result.put(k, ((Enum) v.invoke(annotation)).name());
                } else {
                    result.put(k, v.invoke(annotation));
                }
            } catch (Exception ignore) {
            }
        });
        return result;
    }
}