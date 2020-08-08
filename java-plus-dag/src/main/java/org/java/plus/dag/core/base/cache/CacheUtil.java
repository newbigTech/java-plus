package org.java.plus.dag.core.base.cache;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
//import com.taobao.recommendplatform.protocol.service.ohc.OffHeapCache;
import org.java.plus.dag.core.base.constants.TppCounterNames;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.util.OffHeapCache;
import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Common cache util. HEAP,OFF_HEAP,LRU
 *
 * @author seven.wxy
 * @date 2018/11/15
 */
@SuppressWarnings("unchecked")
public class CacheUtil {
    public static final Row DUMMY_ROW = new Row();
    public static final DataSet<Row> DUMMY_DATA_SET = new DataSet<>();

    private static final String CACHE_TTL_KEY_PREFIX = "_TTL";
    private static final Map HEAP_CACHE = Maps.newConcurrentMap();
    private static final OffHeapCache OFF_HEAP_CACHE = new OffHeapCache();// ServiceFactory.getOffHeapCache();
    private static volatile Map<String, LoadingCache> lruCacheConfigMap = Maps.newConcurrentMap();

    public static <T> void putToCache(String key, T data, CacheType cacheType) {
        putToCache(key, data, cacheType, null);
    }

    public static <T> void putToCache(String key, T data, CacheType cacheType, Long ttlMs) {
        if (CacheType.OFF_HEAP == cacheType) {
            if (Objects.nonNull(ttlMs) && ttlMs > 0) {
                OFF_HEAP_CACHE.put(key, data, ttlMs);
            } else {
                OFF_HEAP_CACHE.put(key, data);
            }
        } else if (CacheType.HEAP == cacheType) {
            //cacheKey + CACHE_TTL_KEY_PREFIX
            if (Objects.nonNull(ttlMs) && ttlMs > 0) {
                OFF_HEAP_CACHE.put(key + CACHE_TTL_KEY_PREFIX, 1, ttlMs);
            } else {
                OFF_HEAP_CACHE.put(key + CACHE_TTL_KEY_PREFIX, 1);
            }
            HEAP_CACHE.put(key, data);
        }
    }

    public static <T> T readFromCache(String key, CacheType cacheType) {
        T result = null;
        if (CacheType.OFF_HEAP == cacheType) {
            result = (T)OFF_HEAP_CACHE.get(key);
        } else if (CacheType.HEAP == cacheType) {
            //cacheKey + CACHE_TTL_KEY_PREFIX
            if (OFF_HEAP_CACHE.get(key + CACHE_TTL_KEY_PREFIX) != null) {
                result = (T)HEAP_CACHE.get(key);
            } else {
                HEAP_CACHE.remove(key);
            }
        } else {
            throw new IllegalArgumentException(String.format("CacheType %s is not support.", cacheType));
        }
        return result;
    }

    public static <K, V> Map<K, V> readFromCache(String cacheInstanceKey, Collection<K> keys, CacheType cacheType,
                                                 Supplier<LruCacheConfig<K, V>> initCacheConfig) {
        Map<K, V> result = Maps.newHashMap();
        Set<K> sets = Sets.newHashSet();
        for (K key : keys) {
            result.compute(key, (k, v) -> {
                V newValue = readFromCache(cacheInstanceKey + k, cacheType);
                if (Objects.isNull(newValue)) {
                    sets.add(k);
                }
                return newValue;
            });
        }
        if (!sets.isEmpty()) {
            LruCacheConfig<K, V> config = initCacheConfig.get();
            Map<K, V> map = config.getLoadAllFunction().apply(sets);
            map.forEach((k, v) -> {
                if (v != null && !(v == DUMMY_ROW || v == DUMMY_DATA_SET || CollectionUtils.sizeIsEmpty(v))) {
                    putToCache(cacheInstanceKey + k, v, cacheType, config.getTimeout() * 1000);
                    result.put(k, v);
                }
            });
        }
        return result;
    }

    /**
     * read value from lru cache
     * confirm initLruCacheSingleton function is init before read value from cache
     *
     * @param cacheInstanceKey lru cache instance key
     * @param keys             cache data keys
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> Map<K, V> readFromLruCache(String cacheInstanceKey, Iterable<K> keys,
                                                    Supplier<LruCacheConfig<K, V>> initCacheConfig) {
        LoadingCache<K, V> cache = getLruCacheSingleton(cacheInstanceKey, initCacheConfig);
        writeCounter(cache, cacheInstanceKey);
        Map<K, V> value = null;
        if (Objects.isNull(cache)) {
            return value;
        }
        try {
            value = cache.getAll(keys);
        } catch (ExecutionException e) {
            Logger.error(cacheInstanceKey + " lru cache get error", e);
        }
        return value;
    }

    private static <K, V> void writeCounter(LoadingCache<K, V> cache, String cacheInstanceKey) {
        // write counter per min
        if (Objects.nonNull(cache) && System.currentTimeMillis() / 1000 % 60 == 0) {
//            ServiceFactory.getTPPCounter()
//                .countAvg(TppCounterNames.LRU_CACHE_SIZE.getCounterName() + cacheInstanceKey, cache.size());
//            ServiceFactory.getTPPCounter()
//                .countAvg(TppCounterNames.LRU_CACHE_HIT_RATE.getCounterName() + cacheInstanceKey, cache.stats().hitRate());
//            ServiceFactory.getTPPCounter()
//                .countAvg(TppCounterNames.LRU_CACHE_LOAD_EXP_RATE.getCounterName() + cacheInstanceKey, cache.stats().loadExceptionRate());
        }
    }

    public static <K, V> void invalidateKeyFromLruCache(String cacheInstanceKey, Iterable<K> keys,
                                                        Supplier<LruCacheConfig<K, V>> initCacheConfig) {
        LoadingCache<K, V> cache = getLruCacheSingleton(cacheInstanceKey, initCacheConfig);
        if (Objects.nonNull(cache)) {
            cache.invalidateAll(keys);
        }
    }

    /**
     * remove all lru cache instance, call this method when solution init
     */
    public static void removeAllLruCache() {
        lruCacheConfigMap.clear();
    }

    /**
     * remove lru cache instance which the key is "cacheInstanceKey"
     *
     * @param cacheInstanceKey lru instance key
     */
    public static void removeLruCache(String cacheInstanceKey) {
        synchronized (cacheInstanceKey.intern()) {
            lruCacheConfigMap.remove(cacheInstanceKey);
        }
    }

    /**
     * get lru cache singleton instance
     *
     * @param cacheInstanceKey lru instance key
     * @param initCacheConfig  lru cache config Supplier
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> LoadingCache<K, V> getLruCacheSingleton(@NotNull String cacheInstanceKey,
                                                                 @NotNull Supplier<LruCacheConfig<K, V>> initCacheConfig) {
        LoadingCache cache = lruCacheConfigMap.get(cacheInstanceKey);
        if (Objects.isNull(cache)) {
            synchronized (cacheInstanceKey.intern()) {
                if (Objects.isNull((cache = lruCacheConfigMap.get(cacheInstanceKey)))) {
                    LruCacheConfig<K, V> lruCacheConfig = initCacheConfig.get();
                    cache = initLruCache(lruCacheConfig.getCacheSize(), lruCacheConfig.getTimeout(), lruCacheConfig.getLoadAllFunction());
                    if (Objects.nonNull(cache)) {
                        lruCacheConfigMap.put(cacheInstanceKey, cache);
                    }
                }
            }
        }
        return cache;
    }

    /**
     * inti lru cache
     *
     * @param cacheSize       cache size
     * @param timeout         cache value ttl(seconds)
     * @param loadAllFunction load value function
     * @param <K>
     * @param <V>
     * @return
     */
    private static <K, V> LoadingCache<K, V> initLruCache(int cacheSize, long timeout,
                                                          Function<Iterable<K>, Map<K, V>> loadAllFunction) {
        return CacheBuilder.newBuilder().concurrencyLevel(8)
            .expireAfterWrite(timeout, TimeUnit.SECONDS).recordStats()
            .maximumSize(cacheSize).build(new CacheLoader<K, V>() {
                @Override
                @ParametersAreNonnullByDefault
                public V load(K key) {
                    Map<K, V> all = loadAllFunction.apply(Lists.newArrayList(key));
                    return all.get(key);
                }

                @Override
                public Map<K, V> loadAll(Iterable<? extends K> keys) {
                    return loadAllFunction.apply((Iterable<K>)keys);
                }
            });
    }
}
