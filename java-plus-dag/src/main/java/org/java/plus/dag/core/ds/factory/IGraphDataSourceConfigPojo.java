package org.java.plus.dag.core.ds.factory;

import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
//import com.taobao.igraph.client.model.KeyList;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.taobao.KeyList;

import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author seven.wxy
 * @date 2018/9/25
 */
@Data
public class IGraphDataSourceConfigPojo {
    private String tableName;
    private String pkey;
    private String[] iGraphSKeys;
    /**
     * the field name in iGraph with specific query. eq: field1,field2
     */
    private String valueField;
    /**
     * the filter pattern when query with iGraph
     */
    private String filter;
    /**
     * iGraph table columns
     */
    private String[] iGraphQueryField;
    /**
     * filed may start with "FieldNameEnum.", AllFieldName.columnName
     */
    private Map<String, String> fieldMapping;

    /**
     * iGraph table column default enum class simple name
     */
    private String defaultFieldClass;
    /**
     * field delimiter
     */
    private String delimiter;
    private Splitter splitter;
    /**
     * the field which need sorted when return from iGraph
     */
    private String orderby;
    /**
     * data set alias, example: co=commit,st=status meaning
     * if iGraph field is commit then we put it in dataSet with name co, each alias is spilt by comma
     */
    private String alias;
    private Map<String, String> aliasMap;
    private int start;
    private int count;
    private List<KeyList> keyLists;
    private boolean async;
    private boolean cacheable;
    /**
     * ע: Ŀǰֻ���첽���ÿ���ʱ��Ч
     */
    private int timeout;
    private boolean isBatch;
    private int batchSize;
    /**
     * ������ֵ ��������� ���� keylist���� / batchSize ����
     */
    private int batchThreshold;
    /**
     * ��¼batch�±�
     */
    private int batchIndex;
    /**
     * ��pKey��hash
     */
    private int hashTableNum;

    /**
     * �ֱ�hashֵ��׺���ӷ�
     */
    private String shardTableSuffixConnector = StringPool.UNDERSCORE;

    /**
     * ָ������pKey��Ҫ���صĽ������
     * ���û��ָ������ôֻ��֤���������������range�Ӿ�涨���������ĵ���pkey���ٻ�������
     * ���ָ����ÿ��pkey����ٻ�count�������
     */
    private int localCount;

    /**
     * ����ʹ����merge, join, atomic����У��������ƽ����ֵͬ�ֶε���Ŀ
     * distinct��֧�ֶ�ֵ����
     * ���Ʒ��ؽ����ֵͬ�ֶ�(���ʽ)����Ŀ��������|count��䣬Ĭ�Ϸ�������Ϊ1
     * ��������Ҫ������෵��3��filed1��ֵͬ���������distinct=field1|3
     */
    private String distinct;

    private JSONObject dbsConfig;

    public IGraphDataSourceConfigPojo() {
    }

    public IGraphDataSourceConfigPojo(String tableName, String pKey, String[] iGraphSKeys, Integer start, Integer count,
                                      String filter, String orderBy, String[] iGraphQueryField,
                                      Map<String, String> aliasMap, String delimiter, boolean async, boolean cacheAble,
                                      int timeout, boolean isBatch, int batchSize, int localCount, String distinct) {
        this.tableName = tableName;
        this.pkey = pKey;
        this.iGraphSKeys = iGraphSKeys;
        this.start = start;
        this.count = count;
        this.filter = filter;
        this.orderby = orderBy;
        this.fieldMapping = parseFieldMapping(iGraphQueryField);
        this.aliasMap = aliasMap;
        this.delimiter = delimiter;
        this.async = async;
        this.cacheable = cacheAble;
        this.timeout = timeout;
        this.isBatch = isBatch;
        this.batchSize = batchSize;
        this.localCount = localCount;
        this.distinct = distinct;
    }

    public IGraphDataSourceConfigPojo(String tableName, Integer start, Integer count,
                                      String filter, String orderBy, String[] iGraphField,
                                      Map<String, String> aliasMap, String delimiter, boolean async, boolean cacheAble,
                                      int timeout, boolean isBatch, int batchSize, int localCount, String distinct) {
        this(tableName, null, null, start, count, filter, orderBy, iGraphField, aliasMap, delimiter,
            async, cacheAble, timeout, isBatch, batchSize, localCount, distinct);
    }

    public IGraphDataSourceConfigPojo(String tableName, Integer start, Integer count,
                                      String filter, String orderBy, String[] iGraphField,
                                      Map<String, String> aliasMap, String delimiter, boolean async, boolean cacheAble,
                                      int timeout, boolean isBatch, int batchSize, int localCount, String distinct,
                                      JSONObject dbsConfig) {
        this(tableName, null, null, start, count, filter, orderBy, iGraphField, aliasMap, delimiter,
                async, cacheAble, timeout, isBatch, batchSize, localCount, distinct);
        this.dbsConfig = dbsConfig;
    }

    public boolean isBatchQuery() {
        return isBatch && batchSize > 0;
    }

    public int getMaxCount() {
        if (CollectionUtils.isEmpty(keyLists)) {
            return count;
        }
        return Math.max(count, keyLists.size());
    }

    public boolean useBatchThreshold() {
        return batchThreshold != IGraphDataSourceUtil.DISABLE_BATCH_THRESHOLD;
    }

    private Map<String, String> parseFieldMapping(String[] fields) {
        String[] iGraphFields = new String[fields.length];
        Map<String, String> result = Maps.newHashMap();
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            if (StringUtils.contains(field, StringPool.C_DOT)) {
                String[] array = StringUtils.split(field, StringPool.C_DOT);
                result.put(array[1], field);
                iGraphFields[i] = array[1];
            } else {
                iGraphFields[i] = field;
            }
        }
        iGraphQueryField = iGraphFields;
        return result;
    }

}

