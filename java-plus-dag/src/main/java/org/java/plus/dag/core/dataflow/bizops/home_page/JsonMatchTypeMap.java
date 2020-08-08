package org.java.plus.dag.core.dataflow.bizops.home_page;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.dataflow.core.Operation;
import org.apache.commons.lang3.StringUtils;

/**
 * DESCRIPTION:
 *
 * @author jaye
 * @date 2019/3/13
 */
public class JsonMatchTypeMap extends Operation<Map<Integer, String>> {
    private String jsonMatchTypeMap;

    public JsonMatchTypeMap(String jsonMatchTypeMap) { this.jsonMatchTypeMap = jsonMatchTypeMap; }

    @Override
    public Map<Integer, String> apply(ProcessorContext ctx) {
        if (StringUtils.isEmpty(jsonMatchTypeMap)) { return new HashMap<>(); }
        Map<Integer, String> result = new HashMap<>();
        try {
            JSONObject matchTypeJson = JSONObject.parseObject(jsonMatchTypeMap);
            for(String key : matchTypeJson.keySet()){
                List<Integer> matchTypeList = (List<Integer>)matchTypeJson.get(key);
                for(Integer beMatchType : matchTypeList) {
                    result.put(beMatchType, key);
                }
            }
        } catch (Exception e) {
            logWarn("parse json match type map faield");
        }
        return result;
    }
}