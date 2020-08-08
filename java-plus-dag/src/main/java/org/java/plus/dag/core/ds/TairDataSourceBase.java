package org.java.plus.dag.core.ds;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.base.utils.tair.ILdbDataMerge;
import org.java.plus.dag.core.base.utils.tair.TairClient;
//import org.java.plus.dag.core.base.utils.tair.TairClientFactory;
import org.java.plus.dag.core.ds.model.ConfigResultPojo;
import org.java.plus.dag.core.ds.model.DataSourceQueryKey;
import org.java.plus.dag.core.ds.model.DataSourceType;
import org.java.plus.dag.core.ds.model.TairDataSourceConfig;
//import com.taobao.tair.ResultCode;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: IGraphDataSourceBase
 * @Package org.java.plus.dag.datasource
 * @date 2018/11/2 ����4:34
 */
public class TairDataSourceBase extends AbstractDataSource implements DataSourceTair {
    @Getter
    protected TairDataSourceConfig tairDataSourceConfig;
    @Getter
    protected TairClient tairClient;
    @Getter
    protected int expire;

    @Override
    protected void setUp(ProcessorConfig processorConfig) {
        tairDataSourceConfig = TairDataSourceConfig.from(processorConfig);
        expire = tairDataSourceConfig.getExpire();
//        tairClient = TairClientFactory.getTairClient(tairDataSourceConfig);
    }

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.TAIR;
    }

    @Override
    public DataSet<Row> read(ProcessorContext processorContext, String pKey, String sKey) {
        ConfigResultPojo configResultPojo = replaceConfig(processorContext);
        return Objects.nonNull(configResultPojo)
            ? internalRead(processorContext, pKey, sKey, configResultPojo.getProcessorConfig())
            : internalRead(processorContext, pKey, sKey, null);
    }

    @Override
    public DataSet<Row> read(ProcessorContext processorContext, List<String> pKeys) {
        ConfigResultPojo configResultPojo = replaceConfig(processorContext);
        return Objects.nonNull(configResultPojo)
            ? internalRead(processorContext, pKeys, configResultPojo.getProcessorConfig())
            : internalRead(processorContext, pKeys, null);
    }

    @Override
    protected DataSet<Row> doRead(ProcessorContext processorContext, ProcessorConfig replaceConfig,
                                  List<DataSourceQueryKey> keyValuePackList) {
        return internalRead(processorContext, Collections.emptyList(), replaceConfig);
    }

    @Override
    protected DataSet<Row> doRead(ProcessorContext processorContext) {
        return internalRead(processorContext, Collections.emptyList(), null);
    }

    @Override
    protected Boolean doWrite(ProcessorContext processorContext, DataSet<Row> dataSet) {
        try {
            dataSet.getData().forEach(row -> tairClient.putData(
                row.getFieldValue(AllFieldName.tair_key),
                row.getFieldValue(AllFieldName.tair_value),
                expire)
            );
            return true;
        } catch (IllegalArgumentException e) {
            Logger.error("put data to tair error ", e);
        }
        return false;
    }

    @Override
    Boolean doWrite(ProcessorContext processorContext, DataSet<Row> dataSet, ProcessorConfig processorConfig) {
        throw new UnsupportedOperationException();
    }

    private TairDataSourceConfig getTairConfig(ProcessorConfig replaceConfig) {
        return Objects.nonNull(replaceConfig) ? TairDataSourceConfig.from(replaceConfig) : this.tairDataSourceConfig;
    }

    private DataSet<Row> internalRead(ProcessorContext processorContext, String pKey, String sKey,
                                      ProcessorConfig replaceConfig) {
        Map<String, Serializable> valueMap = Collections.emptyMap();
        TairDataSourceConfig config = getTairConfig(replaceConfig);
        if (StringUtils.isNotEmpty(config.getPKey()) && StringUtils.isNotEmpty(config.getSKey())) {
            pKey = config.getPKey();
            sKey = config.getSKey();
        }
        if (StringUtils.isNotEmpty(pKey) && StringUtils.isNotEmpty(sKey)) {
            valueMap = new HashMap<>(1);
            valueMap.put(pKey, tairClient.getDataWithPkeySkey(processorContext, pKey, sKey));
        }
        return parseResult(valueMap);
    }

    private DataSet<Row> parseResult(Map<String, Serializable> valueMap) {
        List<Row> rowList = new ArrayList<>(valueMap.size());
        if (MapUtils.isNotEmpty(valueMap)) {
            valueMap.forEach((k, v) -> {
                Map<AllFieldName, Object> returnMap = new HashMap<>(2);
                returnMap.put(AllFieldName.tair_key, k);
                returnMap.put(AllFieldName.tair_value, v);
                rowList.add(new Row(returnMap));
            });
        }
        return new DataSet<>(rowList);
    }

    private DataSet<Row> internalRead(ProcessorContext processorContext, List<String> pKeys,
                                      ProcessorConfig replaceConfig) {
        Map<String, Serializable> valueMap = Collections.emptyMap();
        TairDataSourceConfig config = getTairConfig(replaceConfig);
        if (CollectionUtils.isNotEmpty(pKeys)) {
            valueMap = tairClient.getData(processorContext, pKeys);
        } else if (config != null) {
            String pKey = config.getPKey();
            valueMap = new HashMap<>(1);
            valueMap.put(pKey, tairClient.getData(processorContext, pKey, config.getExpire()));
        }
        return parseResult(valueMap);
    }

    @Override
    public boolean delete(ProcessorContext processorContext, List<String> pkeys) {
        return tairClient.deleteDataBatch(pkeys);
    }

    @Override
    public boolean writeWithVersion(ProcessorContext processorContext, DataSet<Row> dataSet, ILdbDataMerge merge) {
        try {
            dataSet.getData().forEach(row ->
                tairClient.putDataCheckVersion(processorContext, row.getFieldValue(AllFieldName.tair_key),
                    row.getFieldValue(AllFieldName.tair_value), expire, merge)
            );
            return true;
        } catch (IllegalArgumentException e) {
            Logger.error("put data to tair with version error ", e);
        }
        return false;
    }

    @Override
    public boolean writeWithVersion(ProcessorContext processorContext, DataSet<Row> dataSet, ILdbDataMerge merge,
                                    int expireTime) {
        try {
            dataSet.getData().forEach(row ->
                tairClient.putDataCheckVersion(processorContext, row.getFieldValue(AllFieldName.tair_key),
                    row.getFieldValue(AllFieldName.tair_value), expireTime, merge)
            );
            return true;
        } catch (IllegalArgumentException e) {
            Logger.error("put data to tair with version error ", e);
        }
        return false;
    }

    @Override
    public boolean writeWithOutVersionCheck(DataSet<Row> dataSet, int expireTime) {
        try {
            dataSet.getData().forEach(row ->
                tairClient.putData(row.getFieldValue(AllFieldName.tair_key),
                    row.getFieldValue(AllFieldName.tair_value), expireTime)
            );
            return true;
        } catch (IllegalArgumentException e) {
            Logger.error("put data to tair error ", e);
        }
        return false;
    }

    /**
     * write data with version check, if version check error, return immediately
     * @param context
     * @param dataSet
     * @param expireTime
     * @return
     */
    @Override
    public boolean writeWithVersionCheck(ProcessorContext context, DataSet<Row> dataSet, int expireTime) {
        Row row = dataSet.getFirstItem().orElse(new Row());
        String key = row.getFieldValue(AllFieldName.tair_key);
        String value = row.getFieldValue(AllFieldName.tair_value);
        Objects.requireNonNull(key, "put tair key is required");
        Objects.requireNonNull(value, "put tair value is required");
//        ResultCode code = tairClient.putDataWithVersionCheck(context, key, value, expireTime);
        return true;// code.getCode() == ResultCode.VERERROR.getCode() ? false : true;
    }
}
