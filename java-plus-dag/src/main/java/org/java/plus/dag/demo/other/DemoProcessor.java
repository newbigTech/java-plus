package org.java.plus.dag.demo.other;

import java.util.Map;

import org.java.plus.dag.core.base.annotation.ConfigInit;
import org.java.plus.dag.core.base.exception.StatusType;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.proc.BaseProcessor;
import org.java.plus.dag.core.base.utils.Debugger;
 
/**
 * @author seven.wxy
 * @date 2018/9/26
 */
public class DemoProcessor extends BaseProcessor {
    @ConfigInit(desc = "xxParam desc")
    private String xxParam = "xxParamDefaultValue";

    @Override
    public void doInit(ProcessorConfig processorConfig) {
        try {
        } catch (Exception e) {
            Debugger.exception(this, StatusType.PARAM_PARSE_EXCEPTION, e);
        }
    }

    @Override
    public DataSet<Row> doProcess(ProcessorContext processorContext,
                                  DataSet<Row> mainDataSet,
                                  Map<String, DataSet<Row>> otherDataSetList) {
        return mainDataSet;
    }
}
