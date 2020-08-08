package org.java.plus.dag.core.ds.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.utils.Logger;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: PlaceHolderUtil
 * @Package org.java.plus.dag.frame.base.utils
 * @date 2018/10/29 上午11:06
 */
public class PlaceHolderUtil {

    public static final Joiner PATH_JOINER = Joiner.on(".");
    public static final String JSON_PATH_ROOT_PREFIX = "$.";
    public static final String PLACE_HOLDER_PREFIX = "${";
    public static final String ARRAY_L = "[";
    public static final String ARRAY_R = "]";

    public static Set<String> getPlaceHolderSet(ProcessorConfig processorConfig) {

        MapTokenResolver resolver = new MapTokenResolver();
        //序列化
        Reader reader = new TokenReplacingReader(resolver, new StringReader(processorConfig.toJSONString()), "${", "}");

        try {
            int data = reader.read();
            while (data != -1) {
                data = reader.read();
            }
        } catch (IOException e) {

        }
        return resolver.getTokenSet();
    }

    public static ProcessorConfig replacePlaceHolder(ProcessorConfig processorConfig,
                                                     ProcessorContext processorContext) {

        MapTokenResolver resolver = new MapTokenResolver(processorContext.getContextData());
        //序列化
        Reader configSource = new StringReader(processorConfig.toJSONString());
        Reader reader = new TokenReplacingReader(resolver, configSource, "${", "}");

        StringBuilder stringBuilder = new StringBuilder();
        try {
            int data = reader.read();
            while (data != -1) {
                stringBuilder.append((char) data);
                data = reader.read();
            }
        } catch (IOException e) {
            Logger.onlineWarn(ExceptionUtils.getStackTrace(e));
        }
        //反序列化
        return new ProcessorConfig(JSON.parseObject(stringBuilder.toString()));
    }

    public static String fastRemovePlaceHolderMark(String text) {
        int startIdx = text.indexOf("${");
        if (startIdx < 0) {
            return StringUtils.EMPTY;
        }
        startIdx += 2;
        int endIdx = text.indexOf("}", startIdx);
        if (endIdx < 0 || startIdx == endIdx) {
            return StringUtils.EMPTY;
        }
        return text.substring(startIdx, endIdx);
    }

    public static String removePlaceHolderMarker(String text) {
        Reader reader = new TokenReplacingReader(token -> token, new StringReader(text), "${", "}");
        StringBuilder stringBuffer = new StringBuilder();
        int data;
        try {
            data = reader.read();
            while (data != -1) {
                stringBuffer.append((char) data);
                data = reader.read();
            }
            return stringBuffer.toString();
        } catch (IOException e) {
            Logger.warn(() -> "remove place holder marker error=" + e.getMessage());
        }
        return "";
    }

    //替换目标字符串
    public static String replaceHolderWithResovler(String targetString, TokenResolver tokenResolver) {
        try {
            return tokenResolver.resolveToken(targetString);
        } catch (IOException e) {
            return targetString;
        }
    }

    public static Map<String, String> getJsonPathMap(Object objJson) {
        LinkedList<String> path = new LinkedList<>();
        Map<String, String> resultMap = new HashMap<>();
        analysisJson(objJson, resultMap, path);
        return resultMap;
    }

    @SuppressWarnings("rawtypes")
    private static void analysisJson(Object objJson, Map<String, String> result, LinkedList<String> path) {

        //如果obj为json数组
        if (objJson instanceof JSONArray) {
            JSONArray objArray = (JSONArray) objJson;
            for (int i = 0; i < objArray.size(); i++) {
                String last = path.removeLast();
                if (StringUtils.indexOf(last, ARRAY_L) != -1) {
                    last = StringUtils.substringBefore(last, ARRAY_L);
                }
                path.add(last + ARRAY_L + i + ARRAY_R);
                analysisJson(objArray.get(i), result, path);
            }
        }
        //如果为json对象
        else if (objJson instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) objJson;
            for (Object o : jsonObject.keySet()) {
                String key = o.toString();
                Object object = jsonObject.get(key);
                //如果得到的是数组
                if (object instanceof JSONArray) {
                    JSONArray objArray = (JSONArray) object;
                    path.add(key);
                    analysisJson(objArray, result, path);
                }
                //如果key中是一个json对象
                else if (object instanceof JSONObject) {
                    path.add(key);
                    analysisJson(object, result, path);
                    path.removeLast();
                }
                //如果key中是其他
                else {
                    //将 ${ 开头 } 结尾的路径记录下来
                    if (object instanceof String) {
                        String value = (String) object;
                        value = value.trim();
                        //如果有占位符 把此行加入到Map中
                        if (value.contains(PLACE_HOLDER_PREFIX)) {
                            path.add(key);
                            result.put(JSON_PATH_ROOT_PREFIX + PATH_JOINER.join(path),
                                    value);
                            path.removeLast();
                        }
                    }
                }
            }
        }
    }

}
