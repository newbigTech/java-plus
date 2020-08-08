package org.java.plus.dag.core.base.cache;

import java.util.Map;
import java.util.function.Function;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author seven.wxy
 * @date 2019/1/14
 */
@Data
@AllArgsConstructor
public class LruCacheConfig<K, V> {
    int cacheSize;
    long timeout;
    Function<Iterable<K>, Map<K, V>> loadAllFunction;
}
