package org.java.plus.dag.core.operator.join;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterators;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Join right stream create collector util
 * @author seven.wxy
 * @date 2018/9/5
 */
class Cluster<I, K, T> {
    private final Function<I, Optional<Stream<T>>> clusterResolver;

    public Cluster(HashMap<K, List<T>> map, BiPredicate<I, K> matchPredicate) {
        this.clusterResolver = createClusterResolver(map, matchPredicate);
    }

    Optional<Stream<T>> getCluster(I key) {
        return clusterResolver.apply(key);
    }

    private static <I, K, T> Function<I, Optional<Stream<T>>> createClusterResolver(HashMap<K, List<T>> map,
                                                                                    BiPredicate<I, K> matchPredicate) {
        if (matchPredicate == MatchPredicate.EQUALS) {
            return key -> Optional.ofNullable(map.get(key)).map(Collection::stream);
        }
        return key -> emptyIfStreamIsEmpty(map.entrySet().stream()
            .filter(es -> matchPredicate.test(key, es.getKey()))
            .map(Map.Entry::getValue)
            .flatMap(Collection::stream));
    }

    static <T> Optional<Stream<T>> emptyIfStreamIsEmpty(Stream<T> stream) {
        Iterator<T> iterator = stream.iterator();
        if (iterator.hasNext()) {
            return Optional.of(StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false));
        }
        return Optional.empty();
    }

}