package org.java.plus.dag.core.dataflow.ops;

import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.StrUtils;
import org.java.plus.dag.core.dataflow.core.Operation;
import org.apache.commons.lang3.StringUtils;

/**
 * DESCRIPTION:
 *
 * @author jaye
 * @date 2019/3/14
 */
public class GetBeExtendTrigger extends Operation<String> {
    private DataSet<Row> playlog;

    public GetBeExtendTrigger(DataSet<Row> playlog) {
        this.playlog = playlog;
    }

    @Override
    public String apply(ProcessorContext ctx) {
        handleErr(e -> "");
        StringBuilder result = new StringBuilder();
        for (Row row : playlog.getData()) {
            result.append(StrUtils.trimToString(row.getFieldValue(AllFieldName.vdo_id), StringUtils.EMPTY))
                .append(StringPool.COLON)
                .append(StrUtils.trimToString(row.getFieldValue(AllFieldName.ts), StringUtils.EMPTY))
                .append(StringPool.COLON)
                .append(StrUtils.trimToString(row.getFieldValue(AllFieldName.vst_time), StringUtils.EMPTY))
                .append(StringPool.COMMA);
        }
        return result.toString();
    }
}