package org.java.plus.dag.core.dataflow.ops;

import java.util.Map;

import org.java.plus.dag.core.base.em.AlgInfoKey;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.dataflow.core.Operation;

/**
 * DESCRIPTION:
 *
 * @author jaye
 * @date 2019/3/14
 */
public class AppendTriggerTime extends Operation<DataSet<Row>> {
    private Operation<DataSet<Row>> inputOp;
    private DataSet<Row> playlog;

    public AppendTriggerTime(Operation<DataSet<Row>> inputOp, DataSet<Row> playlog) {
        this.inputOp = inputOp;
        this.playlog = playlog;
    }

    @Override
    public DataSet<Row> apply(ProcessorContext ctx) {
        requireOpNonNull(DataSet::new, inputOp);
        handleErr(e -> inputOp.get());
        Map<String, Row> playlogMap = new IdMap(playlog).eval(getCurProcessorName());
        DataSet<Row> input = inputOp.get();
        for (Row row : input.getData()) {
            String triggerId = row.getFieldValue(AllFieldName.triggerItem);
            if (playlogMap.containsKey(triggerId)) {
                Row playlogRow = playlogMap.get(triggerId);
                row.appendAlgInfo(AlgInfoKey.RC_TRIG_TS, playlogRow.getFieldValue(AllFieldName.ts, 0));
                row.appendAlgInfo(AlgInfoKey.RC_TRIG_TIME,
                    playlogRow.getFieldValue(AllFieldName.vst_time, 0));
            }
        }
        return input;
    }
}