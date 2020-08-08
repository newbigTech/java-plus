package org.java.plus.dag.core.base.utils;

import java.util.Map;
import java.util.function.Supplier;

/**
 * @author youku
 */
public class MapUtil {
    public static <K, V> V getOrDefault(Map<K, V> map, K key, Supplier<V> defaultValueSupplier) {
        V v;
        return (((v = map.get(key)) != null) || map.containsKey(key))
                ? v
                : defaultValueSupplier.get();
    }

    public static <K, V> Map<K, V> fluentPut(Map<K, V> map, K key, V value) {
        map.put(key, value);
        return map;
    }

    @SuppressWarnings("unchecked")
    public static <K, V, R extends Map<K, V>> R putAll(Map<K, V> map1, Map<K, V> map2) {
        map1.putAll(map2);
        return (R) map1;
    }
}
