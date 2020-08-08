package org.java.plus.dag.core.ds.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
//import com.taobao.igraph.client.external.fb_app_pg_by_column.FieldValueColumn;
//import com.taobao.igraph.client.external.fb_app_pg_by_column.MatchRecords;
//import com.taobao.igraph.client.model.MatchRecord;
//import com.taobao.igraph.client.model.MatchRecordFBByColumn;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.em.FieldNameEnum;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.EnumUtil;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfig;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfigPojo;
import org.java.plus.dag.core.ds.parser.IGraphSingleQueryResult;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.SetUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: IGraphResponse
 * @Package org.java.plus.dag.frame.ds.parser
 * @date 2018/12/16 5:51 PM
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class IGraphResponse {
    private IGraphAsyncObject iGraphAsyncObject;
    private IGraphBatchQueryResult batchQueryResult;

    private static final Map<String, Map<String, Byte>> igraphFieldMeta = Maps.newConcurrentMap();
    private static final String REFLECT_FILED_NAME = "fbByColumnMatchRecords";

    public IGraphResponse(IGraphAsyncObject iGraphAsyncObject) {
        this.iGraphAsyncObject = iGraphAsyncObject;
    }

    public IGraphResponse(IGraphBatchQueryResult iGraphBatchQueryResult) {
        this.batchQueryResult = iGraphBatchQueryResult;
    }

    public boolean isEmpty() {
        return iGraphAsyncObject == null && batchQueryResult == null;
    }

    public boolean isAsyncEmpty() {
        return iGraphAsyncObject == null || iGraphAsyncObject.getAsyncFunction() == null;
    }

    public boolean isSyncEmpty() {
        return batchQueryResult == null || batchQueryResult.isEmpty();
    }

    public List<Row> getResponseRowList(IGraphDataSourceConfig iGraphDataSourceConfig) {
        if (isSyncEmpty()) {
            return new ArrayList<>();
        }
        List<Row> rowList = new ArrayList<>();
        getBatchQueryResult().getBatchResult().forEach((k, v) -> {
            for (IGraphSingleQueryResult singleQueryResult : v) {
//                rowList.addAll(genResultRow(singleQueryResult.getQueryResult(),
//                    iGraphDataSourceConfig.getAllConfig().get(k), singleQueryResult.getQueryIndex()));
            }
        });
        return rowList;
    }

    protected List<Row> genResultRow(List<Object> recordList, IGraphDataSourceConfigPojo configPojo,
                                     int batchIndex) {
        if (CollectionUtils.isEmpty(recordList)) {
            return new ArrayList<>(0);
        }
        Set<String> returnFields = Sets.newHashSet(configPojo.getIGraphQueryField());
        if (CollectionUtils.isEmpty(returnFields)) {
//            returnFields = recordList.get(0).getFieldName2IndexMap().keySet();
        }
        final Set<String> finalReturnFields = returnFields;
        boolean cacheable = configPojo.isCacheable();
//        MatchRecords fbByColumnMatchRecords = null;
        final String tableName = StringUtils.trimToEmpty(configPojo.getTableName());
        final boolean finalCacheable = true;//cacheable && recordList.get(0) instanceof MatchRecordFBByColumn && StringUtils
//            .isNotEmpty(tableName);
        if (finalCacheable && !finalReturnFields.isEmpty()) {
            Map<String, Byte> values = igraphFieldMeta.get(tableName);
            Set<String> diff = finalReturnFields;
            if (Objects.isNull(values) || !((diff = SetUtils.difference(finalReturnFields,
                igraphFieldMeta.get(tableName).keySet())).isEmpty())) {
                synchronized (tableName.intern()) {
                    values = igraphFieldMeta.get(tableName);
                    if (Objects.isNull(values) || !((diff = SetUtils.difference(finalReturnFields, values.keySet()))
                        .isEmpty())) {
//                        MatchRecordFBByColumn matchRecordFBByColumn = (MatchRecordFBByColumn)recordList.get(0);
                        try {
//                            fbByColumnMatchRecords = (MatchRecords)FieldUtils.readDeclaredField(matchRecordFBByColumn,
//                                REFLECT_FILED_NAME, true);
                        } catch (Exception ignore) {
                        }
//                        if (Objects.nonNull(fbByColumnMatchRecords)) {
//                            Map<String, Integer> fieldName2IndexMap = recordList.get(0).getFieldName2IndexMap();
//                            Map<String, Byte> typeMap = new HashMap<>(fieldName2IndexMap.size());
//                            for (String field : diff) {
//                                typeMap.put(field, getFieldType(fbByColumnMatchRecords, fieldName2IndexMap.get(field)));
//                            }
//                            igraphFieldMeta.put(configPojo.getTableName(), typeMap);
//                        }
                    }
                }
            }
        }
        List<Row> returnList = new ArrayList<>(recordList.size());
        Map<String, Byte> values = igraphFieldMeta.getOrDefault(tableName, Collections.emptyMap());
        Map<String, Integer> fieldName2IndexMap =null;// recordList.get(0).getFieldName2IndexMap();
        recordList.forEach(record -> {
            Map<FieldNameEnum, Object> returnMap = new HashMap<>(finalReturnFields.size());
            finalReturnFields.forEach(field -> {
                String mappingField = configPojo.getFieldMapping().getOrDefault(field, field);
                returnMap.put(replaceFieldWithAlias(mappingField, configPojo),
                    getFieldValue(record, field, fieldName2IndexMap.get(field), values.get(field), finalCacheable));
                }
            );
            Row row = new Row(returnMap);
            row.setFieldValue(AllFieldName.ds_source, configPojo.getTableName());
            row.setFieldValue(AllFieldName.ds_query_index, batchIndex);
            returnList.add(row);
        });
        return returnList;
    }

    private static Object getFieldValue(Object matchRecord, String field, int fieldIndex, Byte columnType,
                                        boolean cacheable) {
//        if (cacheable && columnType != null) {
//            MatchRecordFBByColumn matchRecordFBByColumn = (MatchRecordFBByColumn)matchRecord;
//            Object fieldValue = null;
//            switch (columnType) {
//                case FieldValueColumn.Int8ValueColumn:
//                case FieldValueColumn.Int16ValueColumn:
//                case FieldValueColumn.Int32ValueColumn:
//                case FieldValueColumn.Int64ValueColumn:
//                case FieldValueColumn.UInt8ValueColumn:
//                case FieldValueColumn.UInt16ValueColumn:
//                case FieldValueColumn.UInt32ValueColumn:
//                case FieldValueColumn.UInt64ValueColumn: {
//                    fieldValue = matchRecordFBByColumn.getLong(fieldIndex);
//                    break;
//                }
//                case FieldValueColumn.FloatValueColumn:
//                    fieldValue = matchRecordFBByColumn.getFloat(fieldIndex);
//                    break;
//                case FieldValueColumn.DoubleValueColumn: {
//                    fieldValue = matchRecordFBByColumn.getDouble(fieldIndex);
//                    break;
//                }
//                case FieldValueColumn.StringValueColumn:
//                    fieldValue = matchRecordFBByColumn.getString(fieldIndex, "utf-8");
//                    break;
//                case FieldValueColumn.MultiInt8ValueColumn:
//                case FieldValueColumn.MultiInt16ValueColumn:
//                case FieldValueColumn.MultiInt32ValueColumn:
//                case FieldValueColumn.MultiInt64ValueColumn:
//                case FieldValueColumn.MultiUInt8ValueColumn:
//                case FieldValueColumn.MultiUInt16ValueColumn:
//                case FieldValueColumn.MultiUInt32ValueColumn:
//                case FieldValueColumn.MultiUInt64ValueColumn:
//                    //                    fieldValue = MultiTypeValueWrapper(getLongList(fieldIndex));
//                    //                    break;
//                case FieldValueColumn.MultiFloatValueColumn:
//                case FieldValueColumn.MultiDoubleValueColumn:
//                    //                    fieldValue = MultiTypeValueWrapper(getDoubleList(fieldIndex));
//                    //                    break;
//                case FieldValueColumn.MultiStringValueColumn:
//                    //                    fieldValue = MultiTypeValueWrapper(getStringList(fieldIndex));
//                    fieldValue = matchRecord.getString(field);
//                    break;
//                default:
//                    break;
//            }
//            return fieldValue;
//        } else {
//            return matchRecord.getString(field);
//        }
    	return null;
    }

//    private static byte getFieldType(Object fbByColumnMatchRecords, int fieldIndex) {
//        return null;//fbByColumnMatchRecords.recordColumns(fieldIndex).fieldValueColumnType();
//    }

    private static FieldNameEnum replaceFieldWithAlias(String field,
                                                       IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo) {
        if (iGraphDataSourceConfigPojo == null) {
            return EnumUtil.getEnum(field);
        }
        if (MapUtils.isNotEmpty(iGraphDataSourceConfigPojo.getAliasMap())) {
            String alias = iGraphDataSourceConfigPojo.getAliasMap().get(field);
            if (StringUtils.isNotEmpty(alias)) {
                return getFieldEnum(alias, iGraphDataSourceConfigPojo);
            }
        }
        return getFieldEnum(field, iGraphDataSourceConfigPojo);
    }

    private static FieldNameEnum getFieldEnum(String field,
                                              IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo) {
        if (StringUtils.isNotEmpty(iGraphDataSourceConfigPojo.getDefaultFieldClass())) {
            return EnumUtil.getEnumByDefault(iGraphDataSourceConfigPojo.getDefaultFieldClass(), field);
        } else {
            return EnumUtil.getEnum(field);
        }
    }
}