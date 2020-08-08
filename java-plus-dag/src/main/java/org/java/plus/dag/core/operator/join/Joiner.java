package org.java.plus.dag.core.operator.join;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Stream;

import org.java.plus.dag.core.base.utils.ThreadLocalUtils;

/**
 * Stream joiner basic api
 * @author seven.wxy
 * @date 2018/9/5
 */
public class Joiner {
    /**
     * stream join function
     * @param left left stream
     * @param leftKeyFunction left stream key function
     * @param right right stream
     * @param rightKeyFunction right stream key function
     * @param matchPredicate join on condition
     * @param grouper one to many value group function
     * @param unmatchedLeft unmatched left stream function
     * @param <L>
     * @param <R>
     * @param <KL>
     * @param <KR>
     * @param <Y>
     * @return new stream
     */
    public static <L, R, KL, KR, Y> Stream<Y> join(
        Stream<? extends L> left,
        Function<? super L, KL> leftKeyFunction,
        Stream<? extends R> right,
        Function<? super R, KR> rightKeyFunction,
        BiPredicate<KL, KR> matchPredicate,
        BiFunction<L, Stream<R>, Stream<Y>> grouper,
        Function<? super L, ? extends Y> unmatchedLeft) {

        if (ThreadLocalUtils.isEnableStreamLazy()) {
            LazySupplier<Cluster<KL, KR, R>> rightCluster = LazySupplier.of(() -> createCluster(right, rightKeyFunction, matchPredicate));
            Function<L, Stream<Y>> mangledUnmatchedLeft = mangledUnmatchedLeft(unmatchedLeft);
            return left
                .map(leftElement -> Optional.ofNullable(leftKeyFunction.apply(leftElement))
                    .map(key -> rightCluster.get().getCluster(key)
                        .map((StreamToLeftToResult<R, L, Y>)cluster -> l -> grouper.apply(l, cluster))
                        .orElse(mangledUnmatchedLeft))
                    .orElse(mangledUnmatchedLeft)
                    .apply(leftElement))
                .filter(Objects::nonNull)
                .flatMap(Function.identity());
        } else {
            Cluster<KL, KR, R> rightCluster = createCluster(right, rightKeyFunction, matchPredicate);
            Function<L, Stream<Y>> mangledUnmatchedLeft = mangledUnmatchedLeft(unmatchedLeft);
            return left
                .map(leftElement -> Optional.ofNullable(leftKeyFunction.apply(leftElement))
                    .map(key -> rightCluster.getCluster(key)
                        .map((StreamToLeftToResult<R, L, Y>)cluster -> l -> grouper.apply(l, cluster))
                        .orElse(mangledUnmatchedLeft))
                    .orElse(mangledUnmatchedLeft)
                    .apply(leftElement))
                .filter(Objects::nonNull)
                .flatMap(Function.identity());
        }
    }

    /**
     * Collect right stream first
     * @param right right stream
     * @param rightKeyFunction right stream key function
     * @param matchPredicate join on condition
     * @param <R>
     * @param <KL>
     * @param <KR>
     * @return
     */
    private static <R, KL, KR> Cluster<KL, KR, R> createCluster(Stream<? extends R> right,
                                                                Function<? super R, KR> rightKeyFunction,
                                                                BiPredicate<KL, KR> matchPredicate) {
        return right.collect(ClusterCollector.toCluster(rightKeyFunction, matchPredicate));
    }

    /**
     * unmatched left process function
     * @param unmatchedLeft
     * @param <Y>
     * @param <L>
     * @return
     */
    private static <Y, L> Function<L, Stream<Y>> mangledUnmatchedLeft(Function<? super L, ? extends Y> unmatchedLeft) {
        if (unmatchedLeft == null) {
            return l -> null;
        }
        return l -> Stream.of(unmatchedLeft.apply(l));
    }

    private interface StreamToLeftToResult<R, L, Y> extends Function<Stream<R>, Function<L, Stream<Y>>> {}

}