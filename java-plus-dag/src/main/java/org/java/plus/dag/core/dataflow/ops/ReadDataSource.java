package org.java.plus.dag.core.dataflow.ops;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.TppObjectFactory;
import org.java.plus.dag.core.dataflow.core.Operation;
import org.java.plus.dag.core.ds.AbstractDataSource;
import org.java.plus.dag.core.ds.BEDataSourceBase;
import org.java.plus.dag.core.ds.TairDataSourceBase;

/**
 * ����:
 * <p>
 org.java.plus.dagon.frame.dataflow.ops.ReadDataSource
 *
 * @author jaye
 * @date 2019/2/20
 * <p>
 * config_start:org.java.plus.dagon.frame.dataflow.ops.ReadDataSource||jaye| config_end:
 */
public class ReadDataSource extends Operation<DataSet<Row>> {
    private String dataSourceKey;
    private Consumer<Row> consumer;

    private Operation<Map<String, String>> extraParams;

    private ReadDataSource() {}

    public ReadDataSource(String dataSourceKey) {
        this.dataSourceKey = dataSourceKey;
    }

    public ReadDataSource(String dataSourceKey, Consumer<Row> consumer) {
        this.dataSourceKey = dataSourceKey;
        this.consumer = consumer;
    }

    public ReadDataSource(String dataSourceKey, Operation<Map<String, String>> extraParamOp) {
        this.dataSourceKey = dataSourceKey;
        this.extraParams = extraParamOp;
        depend(extraParamOp);
    }

    private DataSet<Row> readDataSource(ProcessorContext ctx) {
        AbstractDataSource dataSource = TppObjectFactory.getBean(dataSourceKey, AbstractDataSource.class);
        if (null == dataSource) { return new DataSet<>(); }
        DataSet<Row> result;
        if (!(dataSource instanceof TairDataSourceBase)) {
            result = dataSource.read(ctx);
        } else {
            BEDataSourceBase beDataSource = (BEDataSourceBase)dataSource;
            Map<String, String> extraParamInOp = extraParams.get();
            if (null == extraParamInOp) { extraParamInOp = new HashMap<>(); }
            result = beDataSource.read(ctx, extraParamInOp);
        }
        if (null == result) { result = new DataSet<>(); }
        return result;
    }

    @Override
    public DataSet<Row> apply(ProcessorContext ctx) {
        handleErr(e -> new DataSet<>());
        DataSet<Row> result = readDataSource(ctx);
        if (null != consumer) {
            result.getData().forEach(row -> {
                try {
                    consumer.accept(row);
                } catch (Exception e) {
                    logWarn("ReadDataSource: apply function failed, " + e.getMessage());
                }
            });
        }
        return result;
    }
}