package org.java.plus.dag.core.base.model;

import com.alibaba.fastjson.JSONObject;

/**
 * @author seven.wxy
 * @date 2018/9/25
 */
public class ProcessorConfig extends JSONObject {

    public ProcessorConfig() {
    }

    public ProcessorConfig(JSONObject json) {
        super(json);
    }

    public static ProcessorConfig deepfrom(JSONObject json) {
        ProcessorConfig config = new ProcessorConfig();
        json.forEach((key, value) -> {
            if (value instanceof JSONObject) {
                JSONObject innerJsonObject = new JSONObject();
                innerJsonObject.putAll((JSONObject)value);
                config.put(key, innerJsonObject);
            } else {
                config.put(key, value);
            }
        });
        return config;
    }
}
