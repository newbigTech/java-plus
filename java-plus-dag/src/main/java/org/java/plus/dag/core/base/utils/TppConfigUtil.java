package org.java.plus.dag.core.base.utils;

import java.util.Objects;

import com.alibaba.fastjson.JSONObject;

//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
import org.java.plus.dag.core.base.model.ParameterConfig;

/**
 * @author seven.wxy
 * @date 2019/7/3
 */
public class TppConfigUtil {

    public static JSONObject getJson(String key, JSONObject defaultValue) {
        JSONObject currentConfig;
        if (Objects.nonNull(currentConfig = ThreadLocalUtils.getMockTppConfig())) {
            return JsonUtils.getParam(currentConfig, key, defaultValue);
        } else {
//            return ServiceFactory.getTppConfigService().getJson(key, defaultValue);
        }
        return null;
    }

    public static JSONObject getJson(ParameterConfig parameterConfig) {
        return getJson(parameterConfig.getName(), parameterConfig.getDefaultValue());
    }

    public static String getString(String key, String defaultValue) {
        JSONObject currentConfig;
        if (Objects.nonNull(currentConfig = ThreadLocalUtils.getMockTppConfig())) {
            return JsonUtils.getParam(currentConfig, key, defaultValue);
        } else {
//            return ServiceFactory.getTppConfigService().getString(key, defaultValue);
        }
        return null;
    }
}
