package org.java.plus.dag.core.operator.join;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Stream;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.operator.AbstractOperator;
import org.java.plus.dag.core.operator.JoinOperator;

/**
 * common join operator, define one to one, one to many join function
 *
 * @author seven.wxy
 * @date 2018/9/5
 */
public class SimpleJoinOperator extends AbstractOperator implements JoinOperator {
    @Override
    public <KL, KR> DataSet<Row> join(ProcessorContext processorContext,
                                      JoinType joinType,
                                      DataSet<Row> leftDs,
                                      DataSet<Row> rightDs,
                                      Function<Row, KL> leftKeyFunction,
                                      Function<Row, KR> rightKeyFunction,
                                      BiPredicate<KL, KR> joinOn,
                                      BiFunction<Row, Row, Row> combiner) {
        Stream<Row> newStream = joinType == JoinType.INNER ?
            inner(leftDs.getData().stream()).withKey(leftKeyFunction)
                .right(rightDs.getData().stream()).withKey(rightKeyFunction)
                .on(joinOn)
                .combine(combiner)
                .asStream() :
            leftOuter(leftDs.getData().stream()).withKey(leftKeyFunction)
                .right(rightDs.getData().stream()).withKey(rightKeyFunction)
                .on(joinOn)
                .combine(combiner)
                .asStream();
        return DataSet.toDS(newStream);
    }

    /**
     * inner join api
     *
     * @param left left stream
     * @param <L>
     * @return
     */
    public static <L> InnerJoinLeftSide<L> inner(Stream<? extends L> left) {
        checkNotNull(left, "left must not be null");
        return new InnerJoinLeftSide<>(left);
    }

    /**
     * left outer join api
     *
     * @param left left stream
     * @param <L>
     * @return
     */
    public static <L> LeftJoinLeftSide<L> leftOuter(Stream<L> left) {
        checkNotNull(left, "left must not be null");
        return new LeftJoinLeftSide<>(left);
    }

    /**
     * inner join left side define
     *
     * @param <L>
     */
    public static class InnerJoinLeftSide<L> {
        private final Stream<? extends L> left;

        private InnerJoinLeftSide(Stream<? extends L> left) {
            this.left = left;
        }

        public <KL> InnerJoinLeftKey<L, KL> withKey(Function<? super L, KL> leftKeyFunction) {
            checkNotNull(left, "leftKeyFunction must not be null");
            return new InnerJoinLeftKey<>(leftKeyFunction, this);
        }
    }

    /**
     * inner join left key define
     *
     * @param <L>
     * @param <KL>
     */
    public static class InnerJoinLeftKey<L, KL> {
        private final InnerJoinLeftSide<L> leftSide;
        private final Function<? super L, KL> leftKeyFunction;

        private InnerJoinLeftKey(Function<? super L, KL> leftKeyFunction, InnerJoinLeftSide<L> leftSide) {
            this.leftKeyFunction = leftKeyFunction;
            this.leftSide = leftSide;
        }

        public <R> InnerJoinRightSide<L, R, KL> right(Stream<? extends R> right) {
            checkNotNull(right, "right must not be null");
            return new InnerJoinRightSide<>(right, this);
        }
    }

    /**
     * inner join right side define
     *
     * @param <L>
     * @param <R>
     * @param <KL>
     */
    public static class InnerJoinRightSide<L, R, KL> {
        private final Stream<? extends R> right;
        private final InnerJoinLeftKey<L, KL> leftKey;

        private InnerJoinRightSide(Stream<? extends R> right, InnerJoinLeftKey<L, KL> leftKey) {
            this.right = right;
            this.leftKey = leftKey;
        }

        public <KR> InnerJoinRightKey<L, R, KL, KR> withKey(Function<? super R, KR> rightKeyFunction) {
            checkNotNull(rightKeyFunction, "rightKeyFunction must not be null");
            return new InnerJoinRightKey<>(rightKeyFunction, this);
        }
    }

    /**
     * inner join right key define
     *
     * @param <L>
     * @param <R>
     * @param <KL>
     * @param <KR>
     */
    public static class InnerJoinRightKey<L, R, KL, KR> {
        private final Function<? super R, KR> rightKeyFunction;
        private final InnerJoinRightSide<L, R, KL> rightSide;
        private BiPredicate<KL, KR> matchPredicate = MatchPredicate.equals();

        private InnerJoinRightKey(Function<? super R, KR> rightKeyFunction, InnerJoinRightSide<L, R, KL> rightSide) {
            this.rightKeyFunction = rightKeyFunction;
            this.rightSide = rightSide;
        }

        public <Y> InnerJoinApply<L, R, KL, KR, Y> combine(BiFunction<? super L, ? super R, Y> combiner) {
            checkNotNull(combiner, "combiner must not be null");
            return createApply(combinerToGroupMany(combiner));
        }

        public <Y> InnerJoinApply<L, R, KL, KR, Y> group(BiFunction<? super L, Stream<R>, Y> grouper) {
            checkNotNull(grouper, "grouper must not be null");
            return createApply(grouperToGroupMany(grouper));
        }

        private <Y> InnerJoinApply<L, R, KL, KR, Y> createApply(BiFunction<L, Stream<R>, Stream<Y>> groupMany) {
            return new InnerJoinApply<>(rightSide.leftKey.leftSide.left, rightSide.leftKey.leftKeyFunction,
                rightSide.right,
                rightKeyFunction, matchPredicate, groupMany);
        }

        public InnerJoinRightKey<L, R, KL, KR> on(BiPredicate<KL, KR> matchPredicate) {
            checkNotNull(matchPredicate, "matchPredicate must not be null");
            this.matchPredicate = matchPredicate;
            return this;
        }
    }

    /**
     * left join left side define
     *
     * @param <L>
     */
    public static class LeftJoinLeftSide<L> {
        private final Stream<L> left;

        private LeftJoinLeftSide(Stream<L> left) {
            this.left = left;
        }

        public <K> LeftJoinLeftKey<L, K> withKey(Function<L, K> leftKeyFunction) {
            checkNotNull(leftKeyFunction, "leftKeyFunction must not be null");
            return new LeftJoinLeftKey<>(leftKeyFunction, this);
        }
    }

    /**
     * left join left key define
     *
     * @param <L>
     * @param <KL>
     */
    public static class LeftJoinLeftKey<L, KL> {
        private final LeftJoinLeftSide<L> leftSide;
        private final Function<L, KL> leftKeyFunction;

        private LeftJoinLeftKey(Function<L, KL> leftKeyFunction, LeftJoinLeftSide<L> leftSide) {
            this.leftKeyFunction = leftKeyFunction;
            this.leftSide = leftSide;
        }

        public <R> LeftJoinRightSide<L, R, KL> right(Stream<R> right) {
            checkNotNull(right, "right must not be null");
            return new LeftJoinRightSide<>(right, this);
        }
    }

    /**
     * left join right side define
     *
     * @param <L>
     * @param <R>
     * @param <KL>
     */
    public static class LeftJoinRightSide<L, R, KL> {
        private final Stream<R> right;
        private final LeftJoinLeftKey<L, KL> leftKey;

        private LeftJoinRightSide(Stream<R> right, LeftJoinLeftKey<L, KL> leftKey) {
            this.right = right;
            this.leftKey = leftKey;
        }

        public <KR> LeftJoinRightKey<L, R, KL, KR> withKey(Function<R, KR> rightKeyFunction) {
            checkNotNull(rightKeyFunction, "rightKeyFunction must not be null");
            return new LeftJoinRightKey<>(rightKeyFunction, this);
        }
    }

    /**
     * left join right key define
     *
     * @param <L>
     * @param <R>
     * @param <KL>
     * @param <KR>
     */
    public static class LeftJoinRightKey<L, R, KL, KR> {
        private final Function<R, KR> rightKeyFunction;
        private final LeftJoinRightSide<L, R, KL> rightSide;
        private BiPredicate<KL, KR> matchPredicate = MatchPredicate.equals();

        private LeftJoinRightKey(Function<R, KR> rightKeyFunction, LeftJoinRightSide<L, R, KL> rightSide) {
            this.rightKeyFunction = rightKeyFunction;
            this.rightSide = rightSide;
        }

        public <Y> LeftJoinApply<L, R, KL, KR, Y> combine(BiFunction<? super L, ? super R, Y> combiner) {
            checkNotNull(combiner, "combiner must not be null");
            return createApply(combinerToGroupMany(combiner), toUnmatchedLeft(combiner));
        }

        public <Y> LeftJoinApply<L, R, KL, KR, Y> group(BiFunction<? super L, Stream<R>, Y> grouper) {
            checkNotNull(grouper, "grouper must not be null");
            return createApply(grouperToGroupMany(grouper), toUnmatchedLeftStream(grouper));
        }

        public LeftJoinRightKey<L, R, KL, KR> on(BiPredicate<KL, KR> matchPredicate) {
            this.matchPredicate = matchPredicate;
            return this;
        }

        private <Y> LeftJoinApply<L, R, KL, KR, Y> createApply(BiFunction<L, Stream<R>, Stream<Y>> groupMany,
                                                               Function<L, Y> unmatchedLeft) {
            return new LeftJoinApply<>(rightSide.leftKey.leftSide.left, rightSide.leftKey.leftKeyFunction,
                rightSide.right,
                rightKeyFunction, matchPredicate, groupMany, unmatchedLeft);
        }
    }

    /**
     * inner join stream apply process
     *
     * @param <L>
     * @param <R>
     * @param <KL>
     * @param <KR>
     * @param <Y>
     */
    public static class InnerJoinApply<L, R, KL, KR, Y> {
        private final Stream<? extends L> left;
        private final Function<? super L, KL> leftKeyFunction;
        private final Stream<? extends R> right;
        private final Function<? super R, KR> rightKeyFunction;
        private final BiPredicate<KL, KR> matchPredicate;
        private final BiFunction<L, Stream<R>, Stream<Y>> groupMany;
        Function<? super L, ? extends Y> unmatchedLeft;

        private InnerJoinApply(Stream<? extends L> left, Function<? super L, KL> leftKeyFunction,
                               Stream<? extends R> right,
                               Function<? super R, KR> rightKeyFunction, BiPredicate<KL, KR> matchPredicate,
                               BiFunction<L, Stream<R>, Stream<Y>> groupMany) {
            this(left, leftKeyFunction, right, rightKeyFunction, matchPredicate, groupMany, null);
        }

        InnerJoinApply(Stream<? extends L> left, Function<? super L, KL> leftKeyFunction, Stream<? extends R> right,
                       Function<? super R, KR> rightKeyFunction, BiPredicate<KL, KR> matchPredicate,
                       BiFunction<L, Stream<R>, Stream<Y>> groupMany, Function<L, Y> unmatchedLeft) {
            this.left = left;
            this.leftKeyFunction = leftKeyFunction;
            this.right = right;
            this.rightKeyFunction = rightKeyFunction;
            this.matchPredicate = matchPredicate;
            this.groupMany = groupMany;
            this.unmatchedLeft = unmatchedLeft;
        }

        public Stream<Y> asStream() {
            return Joiner.join(
                left,
                leftKeyFunction,
                right,
                rightKeyFunction,
                matchPredicate,
                groupMany,
                unmatchedLeft);
        }
    }

    /**
     * left join stream apply process
     *
     * @param <L>
     * @param <R>
     * @param <KL>
     * @param <KR>
     * @param <Y>
     */
    public static class LeftJoinApply<L, R, KL, KR, Y> extends InnerJoinApply<L, R, KL, KR, Y> {

        private LeftJoinApply(Stream<L> left, Function<L, KL> leftKeyFunction, Stream<R> right,
                              Function<R, KR> rightKeyFunction, BiPredicate<KL, KR> matchPredicate,
                              BiFunction<L, Stream<R>, Stream<Y>> groupMany, Function<L, Y> unmatchedLeft) {
            super(left, leftKeyFunction, right, rightKeyFunction, matchPredicate, groupMany, unmatchedLeft);
        }

        public LeftJoinApply<L, R, KL, KR, Y> withLeftUnmatched(Function<? super L, ? extends Y> unmatchedLeft) {
            checkNotNull(unmatchedLeft, "unmatchedLeft must not be null");
            this.unmatchedLeft = unmatchedLeft;
            return this;
        }
    }

    private static <L, R, Y> BiFunction<L, Stream<R>, Stream<Y>> combinerToGroupMany(
        BiFunction<? super L, ? super R, Y> combiner) {
        return (l, rs) -> rs.map(r -> combiner.apply(l, r));
    }

    private static <L, R, Y> BiFunction<L, Stream<R>, Stream<Y>> grouperToGroupMany(
        BiFunction<? super L, Stream<R>, Y> grouper) {
        return (left, rightStream) -> Stream.of(grouper.apply(left, rightStream));
    }

    private static <L, Y, R> Function<L, Y> toUnmatchedLeft(BiFunction<? super L, ? super R, Y> resultHandler) {
        return l -> resultHandler.apply(l, null);
    }

    private static <L, Y, R> Function<L, Y> toUnmatchedLeftStream(BiFunction<? super L, Stream<R>, Y> resultHandler) {
        return l -> resultHandler.apply(l, Stream.empty());
    }

}