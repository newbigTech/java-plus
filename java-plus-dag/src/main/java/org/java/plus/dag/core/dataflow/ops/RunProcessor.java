package org.java.plus.dag.core.dataflow.ops;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.proc.BaseProcessor;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.base.utils.TppObjectFactory;
import org.java.plus.dag.core.dataflow.core.Operation;

/**
 * ����:
 * <p>
 * org.java.plus.dag.frame.dataflow.ops.RunProcessor
 *
 * @author jaye
 * @date 2019/2/20
 * <p>
 * config_start: |org.java.plus.dag.frame.dataflow.ops.RunProcessor||jaye| config_end:
 */
public class RunProcessor extends Operation<DataSet<Row>> {
    private String processorKey;

    private RunProcessor() {}

    public RunProcessor(String processorKey) { this.processorKey = processorKey; }

    public RunProcessor(String processorKey, List<Operation<DataSet<Row>>> depOps) {
        this.processorKey = processorKey;
        depend((List)depOps);
    }

    private Map<String, DataSet<Row>> getDataSetMap() {
        Map<String, DataSet<Row>> result = new HashMap<>();
        getDependedOps().forEach(op -> {
            if (null != op) {
                result.put(op.getName(), (DataSet<Row>)op.get());
            }
        });
        return result;
    }

    @Override
    public DataSet<Row> apply(ProcessorContext ctx) {
        if (null == processorKey) { return null; }
        BaseProcessor processor = TppObjectFactory.getBean(processorKey, BaseProcessor.class);
        if (null == processor) {
            Logger.warn(() -> "get processor bean failed");
            return null;
        }
        return processor.doProcess(ctx, new DataSet<>(), getDataSetMap());
    }
}