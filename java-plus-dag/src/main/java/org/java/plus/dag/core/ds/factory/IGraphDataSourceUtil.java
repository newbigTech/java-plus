package org.java.plus.dag.core.ds.factory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
//import com.taobao.igraph.client.model.KeyList;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.utils.Debugger;
import org.java.plus.dag.core.base.utils.JsonUtils;
import org.java.plus.dag.core.base.utils.ThreadLocalUtils;
import org.java.plus.dag.taobao.KeyList;

import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: IGraphDataSourceUtil
 * @Package org.java.plus.dag.frame.ds.factory
 * @date 2018/12/14 4:27 PM
 */
public class IGraphDataSourceUtil {
    private static final Splitter.MapSplitter ALIAS_SPLITTER = Splitter.on(",").withKeyValueSeparator("=");
    public static final int DISABLE_BATCH_THRESHOLD = 0;

    public static IGraphDataSourceConfigPojo from(@NonNull ProcessorConfig processorConfig)
        throws IllegalArgumentException {
        return from(processorConfig, null);
    }

    public static IGraphDataSourceConfigPojo from(@NonNull ProcessorConfig processorConfig, List<KeyList> keyList)
        throws IllegalArgumentException {
        try {
            String delimiter = JsonUtils.getParam(processorConfig, "delimiter", StringPool.COMMA);
            String tableName = JsonUtils.getParam(processorConfig, "table_name", "default_table_name");
            String valueField = JsonUtils.getParam(processorConfig, "value_field", StringUtils.EMPTY);
            String[] iGraphField = ArrayUtils.EMPTY_STRING_ARRAY;
            if (StringUtils.isNotBlank(valueField)) {
                iGraphField = StringUtils.split(valueField, delimiter);
            }
            String orderBy = JsonUtils.getParam(processorConfig, "orderby", StringUtils.EMPTY);
            String filter = JsonUtils.getParam(processorConfig, "filter", StringUtils.EMPTY);
            int start = JsonUtils.getParam(processorConfig, "start", 0);
            int count = JsonUtils.getParam(processorConfig, "count", 10);
            String alias = JsonUtils.getParam(processorConfig, "alias", StringUtils.EMPTY);
            Map<String, String> aliasMap = Maps.newHashMap();
            if (StringUtils.isNotEmpty(alias)) {
                aliasMap = ALIAS_SPLITTER.split(alias);
            }
            Boolean solutionIGraphAsync = ThreadLocalUtils.isAsyncIgraph();
            Boolean async = JsonUtils.getParam(processorConfig, "async", solutionIGraphAsync);
            if (Debugger.isLocal()) {
                async = false;
            }
            Boolean cacheAble = JsonUtils.getParam(processorConfig, "cacheable", false);
            Integer solutionIGraphTimeout = ThreadLocalUtils.getAsyncIgraphTimeOut();
            Integer timeout = JsonUtils.getParam(processorConfig, "timeout", solutionIGraphTimeout);
            boolean isBatch = JsonUtils.getParam(processorConfig, "isBatch", false);
            int batchSize = JsonUtils.getParam(processorConfig, "batchSize", 0);
            int localCount = JsonUtils.getParam(processorConfig, "localCount", 0);
            String distinct = JsonUtils.getParam(processorConfig, "distinct", StringUtils.EMPTY);
            String defaultFieldClass = JsonUtils.getParam(processorConfig, "defaultFieldClass", StringUtils.EMPTY);
            JSONObject dbsConfig = processorConfig.getJSONObject("dbsConfig");
            IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo = new IGraphDataSourceConfigPojo(tableName, start,
                count, filter, orderBy, iGraphField, aliasMap, delimiter, async, cacheAble, timeout, isBatch, batchSize,
                localCount, distinct, dbsConfig);
            iGraphDataSourceConfigPojo.setDefaultFieldClass(defaultFieldClass);
            int batchThreshold = JsonUtils.getParam(processorConfig, "batchThreshold", DISABLE_BATCH_THRESHOLD);
            iGraphDataSourceConfigPojo.setBatchThreshold(batchThreshold);
            if (isBatch && batchSize < 1 && batchThreshold < 1) {
                throw new IllegalArgumentException(
                    "batchSize or batchThreshold should not be both below or equal to zero");
            }
            int hashTableNum = Integer.parseInt(JsonUtils.getParam(processorConfig, "hashTableNum", "0"));
            iGraphDataSourceConfigPojo.setHashTableNum(hashTableNum);

            String shardTableSuffixConnector = JsonUtils.getParam(processorConfig, "shardTableSuffixConnector", StringPool.UNDERSCORE);
            iGraphDataSourceConfigPojo.setShardTableSuffixConnector(shardTableSuffixConnector);

            //ʹ�õ��÷���keyList
            if (CollectionUtils.isNotEmpty(keyList)) {
                iGraphDataSourceConfigPojo.setKeyLists(keyList);
                return iGraphDataSourceConfigPojo;
            } else {
                //ͨ��pKey sKey����keyList
                String pKey = JsonUtils.getParam(processorConfig, "pkey", "");
                if (StringUtils.isBlank(pKey)) {
                    return iGraphDataSourceConfigPojo;
                }
                String sKey = JsonUtils.getParam(processorConfig, "skey", "");
                String[] iGraphSKeys = ArrayUtils.EMPTY_STRING_ARRAY;
                if (StringUtils.isNotBlank(sKey)) {
                    iGraphSKeys = sKey.split(delimiter);
                }
                KeyList keyListSingle = genKeyList(pKey, iGraphSKeys);
                iGraphDataSourceConfigPojo.setKeyLists(Arrays.asList(keyListSingle));
                return iGraphDataSourceConfigPojo;
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static KeyList genKeyList(String pkey, String[] skey) {
        if (ArrayUtils.isNotEmpty(skey)) {
            return new KeyList(pkey, skey);
        } else {
            return new KeyList(pkey);
        }
    }

}
