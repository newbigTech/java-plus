package org.java.plus.dag.core.base.utils;

import java.util.Map;
import java.util.Objects;

import com.alibaba.fastjson.JSONObject;

import com.google.common.collect.Maps;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.constants.TppCounterNames;
import org.java.plus.dag.core.base.model.ConfigKey;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import lombok.Getter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

/**
 * @author seven.wxy
 * @date 2018/10/11
 */
public class TppObjectFactory {
    @Getter
    private static volatile Map<String, Object> beanCache = Maps.newConcurrentMap();
    @Getter
    private static volatile Map<String, String> layerBeanKeyCache = Maps.newConcurrentMap();
    private static final String FUNC_SET_INSTANCE_KEY = "setInstanceKey";
    private static final String FUNC_SET_INSTANCE_KEY_PREFIX = "setInstanceKeyPrefix";
    private static final String FUNC_INIT = "init";
    private static final String ERROR_MESSAGE = "configKey is required";

    public static <T> T getBeanExceptionWhenNull(String configKey, Class<T> clazz) {
        T result = getBean(configKey, clazz);
        Objects.requireNonNull(result, configKey + " get bean null");
        return result;
    }

    public static <T> T getBean(String configKey, Class<T> clazz) {
        if (StringUtils.isEmpty(configKey)) {
            return null;
        }
        ConfigKey configKeyObject = getConfigKey(configKey);
        return getBean(configKeyObject.getConfigKey(), configKeyObject.getConfigKeyPrefix(), clazz);
    }

    public static <T> T getBean(String configKey, String configKeyPrefix, Class<T> clazz) {
        if (StringUtils.isEmpty(configKey)) {
            return null;
        }
        ConfigKey configKeyObject = getConfigKey(configKey, configKeyPrefix);
        JSONObject configValue = getConfigValue(configKeyObject, ConstantsFrame.NULL_JSON_OBJECT);
        return getBean(configKeyObject.getConfigKey(), configKeyObject.getConfigKeyPrefix(), configValue, clazz);
    }

    public static <T> T getBean(String configKey, String configKeyPrefix, JSONObject configValue, Class<T> clazz) {
        if (StringUtils.isEmpty(configKey)) {
            return null;
        }
        ConfigKey configKeyObject = getConfigKey(configKey, configKeyPrefix);
        int configValueHashCode = configValue.toString().hashCode();
        String cacheKey = configKeyObject.getConfigKeyPrefix() + StringPool.DASH + configKeyObject.getConfigKey() + StringPool.DASH + configValueHashCode;
        Object beanObj = beanCache.get(cacheKey);
        if (Objects.isNull(beanObj)) {
            String lockKey = lockKey(clazz, cacheKey);
            synchronized (lockKey.intern()) {
                if (Objects.isNull(beanObj = beanCache.get(cacheKey))) {
                    beanObj = createBean(configKeyObject.getConfigKey(), configKeyObject.getConfigKeyPrefix(), configValue);
                    if (Objects.nonNull(beanObj)) {
                        beanCache.put(cacheKey, beanObj);
                    }
                }
            }
        }
        return (T) beanObj;
    }

    public static JSONObject getConfigValue(ConfigKey configKey, JSONObject defaultValue) {
        JSONObject configValue = getConfigValue(configKey.getConfigKey(), configKey.getConfigKeyPrefix(), defaultValue);
        return LayerUtils.replaceManualLayerConfigValue(configKey, configValue);
    }

    public static JSONObject getConfigValue(String configKey, String configKeyPrefix, JSONObject defaultValue) {
        JSONObject result = defaultValue;
        if (StringUtils.isNotEmpty(configKey)) {
            if (StringUtils.isEmpty(configKeyPrefix)) {
                result = LayerUtils.getTppConfig(configKey, null);
                if (Objects.isNull(result)) {
                    result = LayerUtils.getTppConfig(getKeyRemoveDollar(configKey, false), defaultValue);
                }
            } else {
                JSONObject parentConfig = LayerUtils.getTppConfig(configKeyPrefix, defaultValue);
                JSONObject value = parentConfig.getJSONObject(configKey);
                if (Objects.isNull(value)) {
                    value = parentConfig.getJSONObject(getKeyRemoveDollar(configKey, false));
                }
                result = Objects.isNull(value) ? defaultValue : value;
            }
        }
        return result;
    }

    public static String getKeyRemoveDollar(String configKey, boolean fromFirst) {
        String result = configKey;
        int index = fromFirst ? configKey.indexOf(StringPool.DOLLAR) : configKey.lastIndexOf(StringPool.DOLLAR);
        if (index >= 0) {
            result = configKey.substring(0, index);
        }
        return result;
    }

    private static String lockKey(Class clazz, String configKey) {
        return configKey + StringPool.DASH + clazz.hashCode();
    }

    private static Object createBean(String configKey, String configKeyPrefix, JSONObject configValue) {
        Objects.requireNonNull(configKey, ERROR_MESSAGE);
        Object beanObj = null;
        try {
            String className = StringUtils.replace(configKey, StringPool.SLASH, StringPool.DOT);
            className = getKeyRemoveDollar(className, true);
            beanObj = getBeanInstance(ConstantsFrame.PACKAGE_NAME + className);
            ConfigKey configKeyObject = new ConfigKey(configKey, configKeyPrefix);
            String instanceKey = configKeyObject.toString();
            //invoke setInstanceKey method
            MethodUtils.invokeMethod(beanObj, FUNC_SET_INSTANCE_KEY, instanceKey);
            if (configKeyObject.hasPrefix()) {
                MethodUtils.invokeMethod(beanObj, FUNC_SET_INSTANCE_KEY_PREFIX, configKeyPrefix);
            }
            //invoke init method
            MethodUtils.invokeMethod(beanObj, FUNC_INIT, new ProcessorConfig(configValue));
        } catch (Exception e) {
            Logger.onlineWarn(String.format("Error create processor instance, msg:%s, key:%s", e.getMessage(), configKey));
//            ServiceFactory.getTPPCounter().countSum(TppCounterNames.BEAN_CREATE_ERROR.getCounterName(), 1);
            //throw new RuntimeException(instanceKey + " Create error.", e);
            return null;
        }
        return beanObj;
    }

    private static Object getBeanInstance(String classPath, Object... args) {
        Object finalBean = null;
        try {
            Class<?> clazz = Class.forName(classPath);
            if (ArrayUtils.isEmpty(args)) {
                return clazz.newInstance();
            }
            finalBean = ConstructorUtils.invokeConstructor(clazz, args);
        } catch (Exception e) {
            Logger.onlineWarn(String.format("Error create processor instance, msg:%s, classPath:%s", e.getMessage(), classPath));
            // ClassNotFoundException
            //throw new RuntimeException(e);
        }
        return finalBean;
    }

    public static ConfigKey getConfigKey(String configKey) {
        Objects.requireNonNull(configKey, ERROR_MESSAGE);
        String realConfigKey = configKey;
        String[] config = StringUtils.splitByWholeSeparator(configKey, StringPool.HASH);
        String parentConfigKey = StringUtils.EMPTY;
        if (ArrayUtils.isNotEmpty(config) && config.length > 1) {
            realConfigKey = config[1];
            parentConfigKey = config[0];
        }
        return LayerUtils.replaceTppLayerConfigKey(new ConfigKey(realConfigKey, parentConfigKey));
    }

    public static ConfigKey getConfigKey(String configKey, String configKeyPrefix) {
        ConfigKey result;
        Objects.requireNonNull(configKey, ERROR_MESSAGE);
        if (StringUtils.isNotEmpty(configKeyPrefix) && !StringUtils.contains(configKey, StringPool.HASH)) {
            result = LayerUtils.replaceTppLayerConfigKey(new ConfigKey(configKey, configKeyPrefix));
        } else {
            result = getConfigKey(configKey);
        }
        return result;
    }

}