package org.java.plus.dag.core.service.filter;

import java.util.Map;
import java.util.Objects;

import org.java.plus.dag.core.base.annotation.ConfigInit;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.EnumUtil;

/**
 * @author seven.wxy
 * @date 2019/7/8
 */
public class CommonBaseFilter extends BaseFilter {
    @ConfigInit(desc = "in or not in")
    protected Boolean inFilter = true;

    @ConfigInit(desc = "join left field name")
    protected String leftFieldName = "id";
    @ConfigInit(desc = "join right field name")
    protected String rightFieldName = "id";

    @Override
    public DataSet<Row> doProcess(ProcessorContext processorContext, DataSet<Row> mainDataSet,
                                  Map<String, DataSet<Row>> dataSetMap) {
        DataSet<Row> filterDataSet = getDataSetByDefaultProcessorConfigKey(processorContext, mainDataSet, dataSetMap);
        if (Objects.nonNull(filterDataSet) && filterDataSet.isNotEmpty()) {
            if (inFilter) {
                return mainDataSet.in(filterDataSet,
                    (row) -> row.getFieldValue(EnumUtil.getEnum(leftFieldName)),
                    (row) -> row.getFieldValue(EnumUtil.getEnum(rightFieldName)));
            } else {
                return mainDataSet.notIn(filterDataSet,
                    (row) -> row.getFieldValue(EnumUtil.getEnum(leftFieldName)),
                    (row) -> row.getFieldValue(EnumUtil.getEnum(rightFieldName)));
            }
        } else {
            return mainDataSet;
        }
    }
}
