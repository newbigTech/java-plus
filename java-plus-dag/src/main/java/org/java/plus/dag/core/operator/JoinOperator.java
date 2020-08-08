package org.java.plus.dag.core.operator;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.operator.join.JoinType;

/**
 * @author seven.wxy
 * @date 2018/9/28
 */
public interface JoinOperator extends Operator {
    /**
     * Custom join op
     *
     * @param processorContext processor context
     * @param joinType         joinType
     * @param leftDs           left DataSet
     * @param rightDs          right DataSet
     * @param leftKeyFunction  left DataSet join key function
     * @param rightKeyFunction right DataSet join key function
     * @param joinOn           join on predicate
     * @param combiner         join result combiner
     * @param <KL>             left key type
     * @param <KR>             right key type
     * @return new DataSet
     */
    <KL, KR> DataSet<Row> join(ProcessorContext processorContext,
                               JoinType joinType,
                               DataSet<Row> leftDs,
                               DataSet<Row> rightDs,
                               Function<Row, KL> leftKeyFunction,
                               Function<Row, KR> rightKeyFunction,
                               BiPredicate<KL, KR> joinOn,
                               BiFunction<Row, Row, Row> combiner);

}
