package org.java.plus.dag.core.base.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.java.plus.dag.core.base.em.AlgInfoKey;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.em.FieldNameEnum;
import org.java.plus.dag.core.base.model.BaseDto;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.Row;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author seven.wxy
 * @date 2018/10/20
 */
@SuppressWarnings("all")
public class DataParseUtils {
    public static final String ITEM = "item";
    public static final String SCORE = "score";
    public static final String TYPE = "type";
    public static final String MODEL = "model";
    public static final Integer SHOW_INT = 2;

    public static final String VALUE_SPLIT_RULE = "item:type:score:model:ext";
    public static final String VALUE_SPLIT_RULE_ID_SCORE_TYPE = "item:score:type";
    public static final String VALUE_SPLIT_RULE_SCORE_SECOND = "item:score:type:model:ext";
    public static final String VALUE_SPLIT_RULE_ID_TYPE = "item:type";
    public static final String VALUE_SPLIT_RULE_ID_TYPE_SCORE = "item:type:score";

    public static final String ALG_INFO = "alginfo";
    public static final String REQ_ID_PREFIX = "reqid=";

    public static DataSet<Row> parseListToDataSet(List jsonArray) {
        return DataSet.toDS((List) ListUtils.emptyIfNull(jsonArray).stream()
            .filter(e -> e instanceof JSONObject)
            .map(e -> DataParseUtils.parseMapToRow((Map) e))
            .filter(Objects::nonNull).collect(Collectors.toList()));
    }

    public static Row parseMapToRow(Map<String, Object> jsonObject) {
        if (jsonObject == null) {
            return null;
        }
        Row row = new Row();
        jsonObject.forEach((k, v) -> {
            Enum en = null;
            try {
                en = EnumUtil.getEnum(k);
            } catch (Exception ignore) {
            }
            if (en == null) {
                en = EnumUtil.getENUM_CACHE().get(k);
            }
            if (en != null) {
                row.setFieldValue((FieldNameEnum) en, v);
            }
        });
        return row;
    }

    public static List<Map> parseDataSetToItemList(DataSet<Row> dataSet) {
        String prefix = StringUtils.trimToEmpty(SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_ALGINFO_PREFIX));
        return dataSet.getData().stream().map(row -> {
            Map data = Maps.newHashMap();
            BaseDto dto = new BaseDto();
            Map algInfoMap = row.getAlgInfoMap();
            if (MapUtils.isNotEmpty(algInfoMap)) {
                dto.getAlgInfoMap().putAll(algInfoMap);
            }
            dto.genAlgInfo(prefix);
            data.put(AllFieldName.id.name(), row.getId());
            data.put(AllFieldName.type.name(), row.getType());
            data.put(AllFieldName.score.name(), row.getScore());
            data.put(AllFieldName.recext.name(), row.getRecExt());
            data.put(ALG_INFO, dto.getAlgInfo());
            if (MapUtils.isNotEmpty(row.getExtData())) {
                data.put(AllFieldName.extData.name(), row.getExtData());
            }
            return data;
        }).collect(Collectors.toList());
    }

    public static List<BaseDto> parseDataSetToBaseDto(DataSet<Row> dataSet) {
        return dataSet.getData().stream().map(p -> {
            BaseDto dto = new BaseDto(p.getFieldValue(AllFieldName.algInfo));
            dto.setId(p.getId());
            dto.setType(p.getFieldValue(AllFieldName.type, -1));
            dto.setScore(p.getFieldValue(AllFieldName.score, 0.0D));
            dto.setmId(p.getFieldValue(AllFieldName.mId));
            dto.setmHeight(p.getFieldValue(AllFieldName.m_height));
            dto.setmSubtitle(p.getFieldValue(AllFieldName.m_subtitle));
            dto.setmTitle(p.getFieldValue(AllFieldName.m_title));
            dto.setmWidth(p.getFieldValue(AllFieldName.m_width));
            dto.setmUrl(p.getFieldValue(AllFieldName.m_url));
            dto.setRecext(p.getRecExt());
            Map algInfoMap = p.getAlgInfoMap();
            if (MapUtils.isNotEmpty(algInfoMap)) {
                dto.getAlgInfoMap().putAll(algInfoMap);
            }
            if (MapUtils.isNotEmpty(p.getExtData())) {
                dto.getExtData().putAll(p.getExtData());
            }
            return dto;
        }).collect(Collectors.toList());
    }

    public static List<BaseDto> parseDebugDataSetToBaseDto(DataSet<Row> dataSet) {
        return dataSet.getData().stream().map(p -> {
            BaseDto dto = new BaseDto(p.getFieldValue(AllFieldName.algInfo));
            dto.setId(p.getFieldValue(AllFieldName.id, "-1"));
            dto.setType(p.getFieldValue(AllFieldName.type, 0));
            dto.setScore(p.getFieldValue(AllFieldName.score, 0.0));
            dto.setRecext(p.getFieldValue(AllFieldName.recext, "NULL"));
            Map algInfoMap = p.getFieldValue(AllFieldName.algInfoMap);
            if (MapUtils.isNotEmpty(algInfoMap)) {
                dto.getAlgInfoMap().putAll(algInfoMap);
            }
            if (MapUtils.isNotEmpty(p.getFieldValue(AllFieldName.extData))) {
                dto.getExtData().putAll(p.getExtData());
            }
            return dto;
        }).collect(Collectors.toList());
    }

    public static DataSet<Row> parseResult(DataSet<Row> dataSet,
                                           String valueSplitRule,
                                           String valueField,
                                           String defaultItemType,
                                           List<String> extFields) {
        return parseResult(dataSet, valueSplitRule, valueField, defaultItemType, extFields, Collections.emptyList());
    }

    public static DataSet<Row> parseResult(DataSet<Row> dataSet,
                                           String valueSplitRule,
                                           String valueField,
                                           String defaultItemType,
                                           List<String> extFields,
                                           List<String> otherFields) {
        Map<String, Integer> positionMap = getValueSplitRule(valueSplitRule);
        Map<String, Integer> extPositionMap = Maps.newHashMap();
        for (Map.Entry<String, Integer> entry : positionMap.entrySet()) {
            if (!StringUtils.equals(entry.getKey(), ITEM)
                && !StringUtils.equals(entry.getKey(), SCORE)
                && !StringUtils.equals(entry.getKey(), TYPE)
                && !StringUtils.equals(entry.getKey(), MODEL)) {
                extPositionMap.put(entry.getKey(), entry.getValue());
            }
        }
        DataSet<Row> result = new DataSet<>();
        List<Row> rows = dataSet.getData().stream().map(row -> {
            Map<FieldNameEnum, String> extData = Maps.newHashMap();
            String values = row.getFieldValue(EnumUtil.getEnum(valueField));
            if (CollectionUtils.isNotEmpty(extFields)) {
                for (String field : extFields) {
                    FieldNameEnum fieldName = EnumUtil.getEnum(field);
                    String extValue = row.getFieldValue(fieldName);
                    extData.put(fieldName, extValue);
                }
            }
            List<Row> rowList = Lists.newArrayList();
            if (StringUtils.isNotBlank(values)) {
                Iterable<String> valueArr = CommonMethods.COMMA_SPLITTER.split(values);
                for (String value : valueArr) {
                    List<String> splitResult = CommonMethods.COLON_SPLITTER.splitToList(value);
                    Row newRow = new Row();
                    if (MapUtils.isNotEmpty(extData)) {
                        newRow.putEnumKeyMap(extData);
                    }
                    if (positionMap.get(ITEM) != null && splitResult.size() > positionMap.get(ITEM)) {
                        newRow.setId(splitResult.get(positionMap.get(ITEM)));
                    }
                    if (positionMap.get(TYPE) != null && splitResult.size() > positionMap.get(TYPE)) {
                        newRow.setType(Integer.valueOf(splitResult.get(positionMap.get(TYPE))));
                    } else {
                        if (StringUtils.isBlank(defaultItemType)) {
                            newRow.setType(SHOW_INT);
                        } else {
                            newRow.setType(Integer.parseInt(defaultItemType));
                        }
                    }
                    if (positionMap.get(SCORE) != null && splitResult.size() > positionMap.get(SCORE)) {
                        newRow.setScore(Double.valueOf(splitResult.get(positionMap.get(SCORE))));
                    }

                    if (positionMap.get(MODEL) != null && splitResult.size() > positionMap.get(MODEL)) {
                        newRow.setFieldValue(AllFieldName.model, splitResult.get(positionMap.get(MODEL)));
                        newRow.appendAlgInfo(AlgInfoKey.MODEL, splitResult.get(positionMap.get(MODEL)));
                    }
                    if (Objects.nonNull(dataSet.getSource())) {
                        newRow.appendAlgInfo(AlgInfoKey.RC_TABLE, dataSet.getSource());
                    }
                    for (Map.Entry<String, Integer> entry : extPositionMap.entrySet()) {
                        Integer index = entry.getValue();
                        if (splitResult.size() > index) {
                            newRow.setFieldValue(EnumUtil.getEnum(entry.getKey()), splitResult.get(index));
                        }
                    }
                    if (CollectionUtils.isNotEmpty(otherFields)) {
                        for (String field : otherFields) {
                            FieldNameEnum fieldName = EnumUtil.getEnum(field);
                            newRow.setFieldValue(fieldName, row.getFieldValue(fieldName));
                        }
                    }
                    rowList.add(newRow);
                }
            }
            return rowList;
        }).flatMap(List::stream).collect(Collectors.toList());
        result.setData(rows);
        return result;
    }

    public static DataSet<Row> parseResultWithAlgInfo(DataSet<Row> dataSet,
                                                      String valueSplitRule,
                                                      String valueField,
                                                      String defaultItemType,
                                                      String multiValueConnector,
                                                      String fieldValueConnector) {
        Map<String, Integer> positionMap = getValueSplitRule(valueSplitRule);
        Map<String, Integer> algInfoPositionMap = Maps.newHashMap();
        for (Map.Entry<String, Integer> entry : positionMap.entrySet()) {
            if (!StringUtils.equals(entry.getKey(), ITEM)
                && !StringUtils.equals(entry.getKey(), SCORE)
                && !StringUtils.equals(entry.getKey(), TYPE)
                && !StringUtils.equals(entry.getKey(), MODEL)) {
                algInfoPositionMap.put(entry.getKey(), entry.getValue());
            }
        }
        DataSet<Row> result = new DataSet<>();
        Splitter multiValueSplitter = Splitter.on(multiValueConnector);
        Splitter fieldValueSplitter = Splitter.on(fieldValueConnector);
        List<Row> rows = dataSet.getData().stream().map(row -> {
            String values = row.getFieldValue(EnumUtil.getEnum(valueField));
            List<Row> rowList = Lists.newArrayList();
            if (StringUtils.isNotBlank(values)) {
                Iterable<String> valueArr = multiValueSplitter.split(values);
                for (String value : valueArr) {
                    List<String> splitResult = fieldValueSplitter.splitToList(value);
                    Row newRow = new Row();
                    if (positionMap.get(ITEM) != null && splitResult.size() > positionMap.get(ITEM)) {
                        newRow.setId(splitResult.get(positionMap.get(ITEM)));
                    }
                    if (positionMap.get(TYPE) != null && splitResult.size() > positionMap.get(TYPE)) {
                        newRow.setType(Integer.valueOf(splitResult.get(positionMap.get(TYPE))));
                    } else {
                        if (StringUtils.isBlank(defaultItemType)) {
                            newRow.setType(SHOW_INT);
                        } else {
                            newRow.setType(Integer.parseInt(defaultItemType));
                        }
                    }
                    if (positionMap.get(SCORE) != null && splitResult.size() > positionMap.get(SCORE)) {
                        newRow.setScore(Double.valueOf(splitResult.get(positionMap.get(SCORE))));
                    }

                    if (positionMap.get(MODEL) != null && splitResult.size() > positionMap.get(MODEL)) {
                        newRow.setFieldValue(AllFieldName.model, splitResult.get(positionMap.get(MODEL)));
                        newRow.appendAlgInfo(AlgInfoKey.MODEL, splitResult.get(positionMap.get(MODEL)));
                    }
                    if (Objects.nonNull(dataSet.getSource())) {
                        newRow.appendAlgInfo(AlgInfoKey.RC_TABLE, dataSet.getSource());
                    }
                    for (Map.Entry<String, Integer> entry : algInfoPositionMap.entrySet()) {
                        Integer index = entry.getValue();
                        if (splitResult.size() > index) {
                            newRow.appendAlgInfo(AlgInfoKey.valueOf(entry.getKey()), splitResult.get(index));
                        }
                    }
                    rowList.add(newRow);
                }
            }
            return rowList;
        }).flatMap(List::stream).collect(Collectors.toList());
        result.setData(rows);
        return result;
    }

    public static Map<String, Integer> getValueSplitRule(String splitRule) {
        List<String> rules = CommonMethods.COLON_SPLITTER.splitToList(splitRule);
        Map<String, Integer> ruleMap = new HashMap<>(rules.size());
        for (int i = 0; i < rules.size(); i++) {
            ruleMap.put(rules.get(i), i);
        }
        return ruleMap;
    }
}
