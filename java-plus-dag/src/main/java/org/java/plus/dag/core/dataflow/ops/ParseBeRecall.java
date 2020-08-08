package org.java.plus.dag.core.dataflow.ops;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.java.plus.dag.core.base.em.AlgInfoKey;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.em.RecallType;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.dataflow.core.Operation;
import org.apache.commons.lang3.StringUtils;

/**
 * DESCRIPTION:
 *
 * @author jaye
 * @date 2019/3/14
 */
public class ParseBeRecall extends Operation<DataSet<Row>> {
    private static final String DEFAULT_MATCH_TYPE = "UNKNOWN_MATCH_TYPE";

    private DataSet<Row> inputDs;
    private Operation<DataSet<Row>> input;
    private Map<Integer, String> matchTypeMap = new HashMap<>();

    public ParseBeRecall(DataSet<Row> inputDs, Map<Integer,String> matchTypeMap) {
        this.inputDs = inputDs;
        if (null != matchTypeMap) { this.matchTypeMap.putAll(matchTypeMap); }
    }
    public ParseBeRecall(Operation<DataSet<Row>> input, Map<Integer,String> matchTypeMap) {
        this.input = input;
        if (null != matchTypeMap) { this.matchTypeMap.putAll(matchTypeMap); }
    }

    @Override
    public DataSet<Row> apply(ProcessorContext ctx) {
        handleErr(e -> null != inputDs ? inputDs : new DataSet<>());
        if (!isNullOp(input)) { inputDs = input.get(); }
        requireNonNull(inputDs);
        inputDs.getData().forEach(row -> {
            boolean isPoolRecall = false;
            String triggerId = row.getFieldValue(AllFieldName.vdo_id_a);
            String recallId = row.getFieldValue(AllFieldName.vdo_id_b);
            if (null == triggerId) {
                isPoolRecall = true;
                triggerId = row.getFieldValue(AllFieldName.publishid);
                recallId = row.getFieldValue(AllFieldName.itemid);
            }
            requireNonNull(triggerId, recallId);
            double reduceModelScore = row.getFieldValue(AllFieldName.__score__, 0.0D);
            double weight = row.getFieldValue(AllFieldName.weight, 0.0D);
            int matchType = row.getFieldValue(AllFieldName.match_type, 0);
            row.setId(recallId);
            row.setFieldValue(AllFieldName.triggerItem, triggerId);
            row.setFieldValue(AllFieldName.recallCore, weight);
            row.setScore(reduceModelScore);
            row.appendAlgInfo(AlgInfoKey.RC_TRIG, triggerId);
            row.appendAlgInfo(AlgInfoKey.RC_SCORE, weight);
            row.appendAlgInfo(AlgInfoKey.RM_SCORE, reduceModelScore);
            row.appendAlgInfo(AlgInfoKey.RC_BE_TYPE, matchType);
            String algMatchType = matchTypeMap.getOrDefault(matchType, DEFAULT_MATCH_TYPE);
            if (isPoolRecall) {
                row.appendAlgInfo(AlgInfoKey.RC_TYPE, RecallType.PUBLISHID2I.name());
            } else {
                row.appendAlgInfo(AlgInfoKey.RC_TYPE, algMatchType);
            }
            row.setFieldValue(AllFieldName.recallType, RecallType.valueOf(algMatchType));
            Long triggerNum = row.getFieldValue(AllFieldName.trigger_num);
            String matchTypeList = row.getFieldValue(AllFieldName.match_type_list, StringUtils.EMPTY);
            if (Objects.nonNull(triggerNum)) {
                row.appendAlgInfo(AlgInfoKey.RC_TRIG_NUM, triggerNum);
            }
            if (StringUtils.isNotBlank(matchTypeList)) {
                row.appendAlgInfo(AlgInfoKey.RC_BETYPE_LIST, matchTypeList);
            }
        });
        return inputDs;
    }
}