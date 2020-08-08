package org.java.plus.dag.core.base.utils;

import java.util.Objects;

import org.java.plus.dag.solution.Context;

import com.alibaba.fastjson.util.TypeUtils;

//import com.taobao.recommendplatform.protocol.solution.Context;

/**
 * Context value is string...
 * @author seven.wxy
 * @date 2018/10/10
 */
public class ContextUtil {
    public static String getString(Context context, String key) {
        String result = null;
        try {
            if (Objects.nonNull(context)) {
                result = TypeUtils.castToString(context.get(key));
            }
        } catch (Exception e) {
        }
        return result;
    }

    public static Boolean getBoolean(Context context, String key) {
        Boolean result = null;
        try {
            if (Objects.nonNull(context)) {
                result = TypeUtils.castToBoolean(context.get(key));
            }
        } catch (Exception e) {
        }
        return result;
    }

    public static Double getDouble(Context context, String key) {
        Double result = null;
        try {
            if (Objects.nonNull(context)) {
                result = TypeUtils.castToDouble(context.get(key));
            }
        } catch (Exception e) {
        }
        return result;
    }

    public static Integer getInt(Context context, String key) {
        Integer result = null;
        try {
            if (Objects.nonNull(context)) {
                result = TypeUtils.castToInt(context.get(key));
            }
        } catch (Exception e) {
        }
        return result;
    }

    public static Long getLong(Context context, String key) {
        Long result = null;
        try {
            if (Objects.nonNull(context)) {
                result = TypeUtils.castToLong(context.get(key));
            }
        } catch (Exception e) {
        }
        return result;
    }

    public static String getStringOrDefault(Context context, String key, String defaultValue) {
        String result = defaultValue;
        try {
            if (Objects.nonNull(context)) {
                result = TypeUtils.castToString(context.get(key));
            }
            if (Objects.isNull(result)) {
                result = defaultValue;
            }
        } catch (Exception e) {
        }
        return result;
    }

    public static Boolean getBooleanOrDefault(Context context, String key, Boolean defaultValue) {
        Boolean result = defaultValue;
        try {
            if (Objects.nonNull(context)) {
                result = TypeUtils.castToBoolean(context.get(key));
            }
            if (Objects.isNull(result)) {
                result = defaultValue;
            }
        } catch (Exception e) {
        }
        return result;
    }

    public static Double getDoubleOrDefault(Context context, String key, Double defaultValue) {
        Double result = defaultValue;
        try {
            if (Objects.nonNull(context)) {
                result = TypeUtils.castToDouble(context.get(key));
            }
            if (Objects.isNull(result)) {
                result = defaultValue;
            }
        } catch (Exception e) {
        }
        return result;
    }

    public static Integer getIntOrDefault(Context context, String key, Integer defaultValue) {
        Integer result = defaultValue;
        try {
            if (Objects.nonNull(context)) {
                result = TypeUtils.castToInt(context.get(key));
            }
            if (Objects.isNull(result)) {
                result = defaultValue;
            }
        } catch (Exception e) {
        }
        return result;
    }

    public static Long getLongOrDefault(Context context, String key, Long defaultValue) {
        Long result = defaultValue;
        try {
            if (Objects.nonNull(context)) {
                result = TypeUtils.castToLong(context.get(key));
            }
            if (Objects.isNull(result)) {
                result = defaultValue;
            }
        } catch (Exception e) {
        }
        return result;
    }
}
