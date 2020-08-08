package org.java.plus.dag.core.base.utils;

import com.alibaba.fastjson.JSONObject;

import org.java.plus.dag.core.base.constants.ConstantsFrame;

/**
 * @author seven.wxy
 * @date 2018/10/22
 */
public class SolutionConfigUtil {
    public static <T> T getSolutionConfig(SolutionConfig config) {
        boolean configMatch = SolutionConfig.SOLUTION_MULTI_THREAD_EXECUTE == config || SolutionConfig.SERVICE_ASYNC_BE == config
            || SolutionConfig.SERVICE_ASYNC_IGRAPH == config;
        boolean returnFalse = Debugger.isLocal() && configMatch;
        if (returnFalse) {
            return (T)Boolean.FALSE;
        }
        JSONObject configValue = LayerUtils.getTppConfig(SolutionConfig.SOLUTION_CONFIG.getName(), ConstantsFrame.NULL_JSON_OBJECT);
        return JsonUtils.getParam(configValue, config);
    }
}
