//package org.java.plus.dag.core.base.utils.be;
//
//import com.alibaba.fastjson.JSONObject;
//import com.google.common.collect.Maps;
//import com.google.gson.JsonElement;
//import com.taobao.recommendplatform.protocol.service.dii.DIIResponse;
//import org.java.plus.dag.core.base.em.AllFieldName;
//import org.java.plus.dag.core.ds.model.BEDataSourceConfig;
//import org.java.plus.dag.core.base.model.Row;
//import org.java.plus.dag.core.base.utils.Logger;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * @author seth.zjw
// * @version V1.0
// * @Title: JsonBEParser
// * @Package org.java.plus.dag.frame.base.utils.be
// * @date 2018/10/10 ����2:09
// */
//public class JsonBEParser implements BEParser {
//    private String outfmt;
//    private Map<String, String> aliasMap;
//
//    public JsonBEParser(BEDataSourceConfig beDataSourceConfig) {
//        outfmt = beDataSourceConfig.getOutfmt();
//        aliasMap = beDataSourceConfig.getAliasMap();
//    }
//
//    @Override
//    public JSONObject parseResponse(DIIResponse response) {
//        JSONObject jObj = new JSONObject();
//        try {
//            DIIResponse.MatchItems matchItems = response.getMatch_items();
//            jObj.put("field_names", matchItems.getField_names());
//            jObj.put("field_values", matchItems.getField_values());
//            jObj.put("outfmt", outfmt);
//        } catch (Exception ex) {
//            Logger.error(ex.getMessage(), ex);
//        }
//        return jObj;
//    }
//
//    @Override
//    public List<Row> parseDetailResponse(DIIResponse response) {
//        List<Row> rowList = new ArrayList<>();
//        DIIResponse.MatchItems matchItems = response.getMatch_items();
//        List<String> fields = matchItems.getField_names();
//        List<List<JsonElement>> fieldValues = matchItems.getField_values();
//        Map<Integer, String> index2FieldMap = Maps.newHashMap();
//        for (int i = 0; i < fields.size(); i++) {
//            index2FieldMap.put(i, fields.get(i));
//        }
//        for (List<JsonElement> innerList : fieldValues) {
//            Map<AllFieldName, Object> oneRecordmap = new HashMap<>(innerList.size());
//            for (int j = 0; j < innerList.size(); j++) {
//                JsonElement jEle = innerList.get(j);
//                String fieldName = index2FieldMap.get(j);
//                if(aliasMap != null && aliasMap.containsKey(fieldName)){
//                    fieldName = aliasMap.get(fieldName);
//                }
//                oneRecordmap.put(AllFieldName.valueOf(fieldName), jEle.getAsString());
//            }
//            Row row = new Row(oneRecordmap);
//            rowList.add(row);
//        }
//        return rowList;
//    }
//}
