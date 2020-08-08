package org.java.plus.dag.core.ds;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.java.plus.dag.core.base.annotation.ConfigInit;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.proc.BaseProcessor;
import org.java.plus.dag.core.base.utils.JsonUtils;
import org.java.plus.dag.core.ds.model.ConfigResultPojo;
import org.java.plus.dag.core.ds.model.DataSourceOperationType;
import org.java.plus.dag.core.ds.model.DataSourceQueryKey;
import org.java.plus.dag.core.ds.model.PlaceHolderDO;
import org.java.plus.dag.core.ds.parser.ConfigParser;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

/**
 * @author seven.wxy
 * @date 2018/9/25
 */
public abstract class AbstractDataSource extends BaseProcessor implements DataSource {
    protected static final String OTHER_DATA_SET_KEY = "other_dataSet_key";

    protected PlaceHolderDO placeHolderDO;

    @ConfigInit(desc = "true=read, false=write")
    protected Boolean read = true;

    @Setter
    @Getter
    protected String parentInstanceKey = StringUtils.EMPTY;

    @Override
    public void doInit(ProcessorConfig processorConfig) {
        //初始化配置
        setUp(processorConfig);
        //构建占位符对象
        placeHolderDO = PlaceHolderDO.from(processorConfig);
    }

    @Override
    public DataSet<Row> read(ProcessorContext processorContext) {
        return internalRead(processorContext, null);
    }

    @Override
    public DataSet<Row> read(ProcessorContext processorContext, DataSet<Row> dataSet) {
        return internalRead(processorContext, dataSet);
    }

    @Override
    public Boolean write(ProcessorContext processorContext, DataSet<Row> dataSet) {
        return doWrite(processorContext, dataSet);
    }

    @Override
    public DataSet<Row> doProcess(ProcessorContext processorContext, DataSet<Row> mainDataSet) {
        if (read) {
            return read(processorContext, mainDataSet);
        } else {
            write(processorContext, mainDataSet);
            return mainDataSet;
        }
    }

    @Override
    public DataSet<Row> doProcess(ProcessorContext processorContext, DataSet<Row> mainDataSet,
                                  Map<String, DataSet<Row>> otherDataSetMap) {
        if (read) {
            return useDataSet()
                ? read(processorContext, otherDataSetMap.get(getDataSetKey()))
                : read(processorContext, mainDataSet);
        } else {
            write(processorContext, mainDataSet);
            return mainDataSet;
        }
    }

    /**
     * DataSource init method, invoke when bean create
     *
     * @param processorConfig
     */
    abstract void setUp(ProcessorConfig processorConfig);

    /**
     * read data with context param replace
     *
     * @param processorContext
     * @return
     */
    abstract DataSet<Row> doRead(ProcessorContext processorContext);

    /**
     * read data with keyValuePackList
     *
     * @param processorContext
     * @param newConfig
     * @param keyValuePackList
     * @return
     */
    abstract DataSet<Row> doRead(ProcessorContext processorContext, ProcessorConfig newConfig, List<DataSourceQueryKey> keyValuePackList);

    /**
     * write data to DataSource
     *
     * @param processorContext
     * @param dataSet
     * @return
     */
    abstract Boolean doWrite(ProcessorContext processorContext, DataSet<Row> dataSet);

    /**
     * write data to DataSource with config
     *
     * @param processorContext
     * @param dataSet
     * @param processorConfig
     * @return
     */
    abstract Boolean doWrite(ProcessorContext processorContext, DataSet<Row> dataSet, ProcessorConfig processorConfig);

    protected ConfigResultPojo replaceConfig(ProcessorContext processorContext) {
        return ConfigParser.replaceToProcessConfig(processorConfig, processorContext, placeHolderDO);
    }

    private DataSet<Row> internalRead(ProcessorContext processorContext, DataSet<Row> dataSet) {
        //从dataSet获取keyList 和 替换后的配置
        ConfigResultPojo resultPojo = ConfigParser.replaceToProcessConfig(processorConfig, processorContext, dataSet,
            placeHolderDO, DataSourceOperationType.READ, getDataSourceType());
        if (resultPojo == null || resultPojo.noData()) {
            return doRead(processorContext);
        } else {
            return doRead(processorContext, resultPojo.getProcessorConfig(), resultPojo.getDataSourceQueryKeyList());
        }
    }

    private boolean useDataSet() {
        return StringUtils.isNotBlank(getDataSetKey());
    }

    private String getDataSetKey() {
        return Objects.isNull(processorConfig)
            ? StringUtils.EMPTY
            : JsonUtils.getParam(processorConfig, OTHER_DATA_SET_KEY, StringUtils.EMPTY);
    }
}
