//package org.java.plus.dag.core.base.utils.be;
//
//import com.alibaba.fastjson.JSONObject;
//import com.taobao.recommendplatform.protocol.service.dii.DIIResponse;
//import org.java.plus.dag.core.base.em.AllFieldName;
//import org.java.plus.dag.core.ds.model.BEDataSourceConfig;
//import org.java.plus.dag.core.base.model.Row;
//import org.java.plus.dag.core.base.utils.Logger;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
///**
// * @author seth.zjw
// * @version V1.0
// * @Title: FB2BEParser
// * @Package org.java.plus.dag.utils
// * @date 2018/10/10 ����2:08
// */
//public class FB2BEParser implements BEParser {
//    private String outfmt;
//    private Map<String, String> aliasMap;
//
//    public FB2BEParser(BEDataSourceConfig beDataSourceConfig) {
//        outfmt = beDataSourceConfig.getOutfmt();
//        aliasMap = beDataSourceConfig.getAliasMap();
//    }
//
//    @Override
//    public JSONObject parseResponse(DIIResponse response) {
//        JSONObject jObj = new JSONObject();
//        try {
//            DIIResponse.MatchColumnItems matchItems = response.getMatchColumnItems();
//            List<String> fields = matchItems.getFieldNames();
//            jObj.put("field_names", fields);
//            jObj.put("field_values", matchItems.getColumnItems());
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
//
//        DIIResponse.MatchColumnItems matchItems = response.getMatchColumnItems();
//        List<String> fields = matchItems.getFieldNames();
//        if (fields != null) {
//            for (String column : fields) {
//                DIIResponse.IColumnItem columnItem = matchItems.getColumn(column);
//                if (columnItem != null) {
//                    if(aliasMap != null && aliasMap.containsKey(column)){
//                        column = aliasMap.get(column);
//                    }
//                    for (int i = 0,size = columnItem.size(); i < size; i++) {
//                        if (rowList.size() - 1 < i) {
//                            rowList.add(i, new Row());
//                        }
//                        rowList.get(i).setFieldValue(AllFieldName.valueOf(column), columnItem.value(i));
//                    }
//                }
//            }
//        }
//        return rowList;
//    }
//}
