package org.java.plus.dag.core.dataflow.ops;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.dataflow.core.Operation;

/**
 * DESCRIPTION:
 *
 * @author jaye
 * @date 2019/2/27
 */
public class AssignRankScore extends Operation<DataSet<Row>> {
    private BiConsumer<Row, Row> assigner;
    private Consumer<Row> emptyRankHandler;
    private Operation<DataSet<Row>> resultOp;
    private Operation<DataSet<Row>> scoreOp;

    private AssignRankScore() {}

    public AssignRankScore(Operation<DataSet<Row>> resultOp, Operation<DataSet<Row>> scoreOp) {
        this.resultOp = resultOp;
        this.scoreOp = scoreOp;
        this.assigner = (result, rank) -> result.setScore(rank.getScore());
        this.emptyRankHandler = row -> {};
        depend(resultOp, scoreOp);
    }

    public AssignRankScore(Operation<DataSet<Row>> resultOp, Operation<DataSet<Row>> scoreOp,
                           BiConsumer<Row, Row> assigner) {
        this.resultOp = resultOp;
        this.scoreOp = scoreOp;
        this.assigner = assigner;
        this.emptyRankHandler = row -> {};
        depend(resultOp, scoreOp);
    }

    public AssignRankScore(Operation<DataSet<Row>> resultOp, Operation<DataSet<Row>> scoreOp,
                           BiConsumer<Row, Row> assigner, Consumer<Row> emptyRankHandler) {
        this.resultOp = resultOp;
        this.scoreOp = scoreOp;
        this.assigner = assigner;
        this.emptyRankHandler = emptyRankHandler;
        depend(resultOp, scoreOp);
    }

    private Map<String, Row> mapRankScore(DataSet<Row> rankDs) {
        Map<String, Row> rankMap = new HashMap<>(rankDs.getData().size());
        rankDs.getData().forEach(row -> rankMap.put(row.getId(), row));
        return rankMap;
    }

    @Override
    public DataSet<Row> apply(ProcessorContext ctx) {
        requireOpNonNull(resultOp, scoreOp);
        DataSet<Row> dsToAssign = resultOp.get();
        DataSet<Row> rankDs = scoreOp.get();
        if (rankDs.getData().isEmpty()) {
            dsToAssign.getData().forEach(row -> emptyRankHandler.accept(row));
            return dsToAssign;
        }
        Map<String, Row> rankMap = mapRankScore(rankDs);
        dsToAssign.getData().forEach(row -> assigner.accept(row, rankMap.get(row.getId())));
        return dsToAssign;
    }
}