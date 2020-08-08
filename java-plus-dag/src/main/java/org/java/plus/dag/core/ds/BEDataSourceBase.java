package org.java.plus.dag.core.ds;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.Debugger;
//import org.java.plus.dag.core.base.utils.be.BEClient;
import org.java.plus.dag.core.ds.model.BEDataSourceConfig;
import org.java.plus.dag.core.ds.model.ConfigResultPojo;
import org.java.plus.dag.core.ds.model.DataSourceQueryKey;
import org.java.plus.dag.core.ds.model.DataSourceType;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: IGraphDataSourceBase
 * @Package org.java.plus.dag.datasource
 * @date 2018/11/2 ����4:34
 */
public class BEDataSourceBase extends AbstractDataSource implements DataSourceBE {
    private BEDataSourceConfig beDataSourceConfig;

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.BE;
    }

    @Override
    public DataSet<Row> read(ProcessorContext processorContext, Map<String, String> paramMap) {
        ConfigResultPojo configResultPojo = replaceConfig(processorContext);
        return internalRead(processorContext, configResultPojo.getProcessorConfig(), paramMap);
    }

    @Override
    protected void setUp(ProcessorConfig processorConfig) {
        beDataSourceConfig = BEDataSourceConfig.from(processorConfig);
    }

    @Override
    protected DataSet<Row> doRead(ProcessorContext processorContext) {
        return internalRead(processorContext, null, null);
    }

    @Override
    protected DataSet<Row> doRead(ProcessorContext processorContext, ProcessorConfig newConfig,
                                  List<DataSourceQueryKey> keyValuePackList) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Boolean doWrite(ProcessorContext processorContext, DataSet<Row> dataSet) {
        throw new UnsupportedOperationException();
    }

    @Override
    Boolean doWrite(ProcessorContext processorContext, DataSet<Row> dataSet, ProcessorConfig processorConfig) {
        throw new UnsupportedOperationException();
    }

    private DataSet<Row> internalRead(ProcessorContext processorContext, ProcessorConfig replaceConfig,
                                      Map<String, String> paramMap) {
        BEDataSourceConfig beDataSourceConfig = Objects.nonNull(replaceConfig)
            ? BEDataSourceConfig.from(replaceConfig) : this.beDataSourceConfig;
        if (Debugger.isLocal()) {
            beDataSourceConfig.setOutfmt(ConstantsFrame.JSON);
            beDataSourceConfig.setTimeout(ConstantsFrame.PROCESSOR_DEBUG_TIMEOUT_MS);
        }
//        BEClient beClient = new BEClient(beDataSourceConfig, paramMap, this.getParentInstanceKey());
        return null;//beClient.retrieveDataSet(processorContext, beDataSourceConfig.isAsync());
    }

}
