package org.java.plus.dag.core.operator.join;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Custom collector to process join right stream
 * @author seven.wxy
 * @date 2018/9/5
 */
class ClusterCollector {
    static <I, K, T> Collector<? super T, HashMap<K, List<T>>, Cluster<I, K, T>> toCluster(
        Function<? super T, K> classifier,
        BiPredicate<I, K> matchPredicate) {

        return new Collector<T, HashMap<K, List<T>>, Cluster<I, K, T>>() {

            /**
             * create new result object
             * @return
             */
            @Override
            public Supplier<HashMap<K, List<T>>> supplier() {
                return HashMap::new;
            }

            /**
             * add item to result
             * @return
             */
            @Override
            public BiConsumer<HashMap<K, List<T>>, T> accumulator() {
                return (map, item) -> Optional.ofNullable(classifier.apply(item))
                    .map(key -> map.computeIfAbsent(key, k -> new ArrayList<>()))
                    .ifPresent(cluster -> cluster.add(item));
            }

            /**
             * merge result list to one
             * @return
             */
            @Override
            public BinaryOperator<HashMap<K, List<T>>> combiner() {
                return (map, other) -> {
                    other.forEach((key, otherCluster) ->
                        map.merge(key, otherCluster, (left, right) -> {
                            left.addAll(right);
                            return left;
                        }));
                    return map;
                };
            }

            /**
             * convert result
             * @return
             */
            @Override
            public Function<HashMap<K, List<T>>, Cluster<I, K, T>> finisher() {
                return map -> new Cluster<>(map, matchPredicate);
            }

            /**
             * support concurrent or not
             * @return
             */
            @Override
            public Set<Characteristics> characteristics() {
                return EnumSet.of(Characteristics.UNORDERED);
            }
        };
    }
}