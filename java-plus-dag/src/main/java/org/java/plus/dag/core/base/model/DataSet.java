package org.java.plus.dag.core.base.model;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.java.plus.dag.core.base.cache.CacheType;
import org.java.plus.dag.core.base.cache.CacheUtil;
import org.java.plus.dag.core.base.em.AlgInfoKey;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.em.FieldNameEnum;
import org.java.plus.dag.core.base.utils.FrameCollectionUtils;
import org.java.plus.dag.core.base.utils.ThreadLocalUtils;
import org.java.plus.dag.core.base.utils.TppObjectFactory;
import org.java.plus.dag.core.operator.MultiDataSetOperator;
import org.java.plus.dag.core.operator.PersistOperator;
import org.java.plus.dag.core.operator.ShuffleOperator;
import org.java.plus.dag.core.operator.SingleDataSetOperator;
import org.java.plus.dag.core.operator.SortOperator;
import org.java.plus.dag.core.operator.join.MatchPredicate;
import org.java.plus.dag.core.operator.join.SimpleJoinOperator;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

/**
 * Stream Lazy process DataSet
 * <pre>{@code
 * DataSet<Row> leftDataSet = new DataSet();
 * DataSet<Row> rightDataSet = new DataSet();
 * List<Row> rowList = leftDataSet.innerJoin(rightDataSet, Row::getId, Row::getId)
 *     .filter(row -> row.getScore() > 0.8D)
 *     .sort(Comparator.comparing(Row::getScore).reversed())
 *     .limit(20)
 *     .getData();
 *
 * }</pre>
 *
 * @author seven.wxy
 * @date 2018/9/6
 */
public class DataSet<T extends Row> implements Serializable {
    private static final long serialVersionUID = -8393123412006697677L;

    public static Field streamLinkedOrConsumedField;
    private static final String STREAM_CONSUMED_FIELD_NAME = "linkedOrConsumed";

    static {
        try {
            Class streamClass = Stream.empty().getClass();
            streamLinkedOrConsumedField = FieldUtils.getField(streamClass, STREAM_CONSUMED_FIELD_NAME, true);
        } catch (Exception e) {
        }
    }

    /**
     * DataSet inner data list
     */
    private List<T> data = Lists.newArrayList();
    /**
     * DataSet inner {@link #data} list stream supplier
     */
    private Supplier<Stream<T>> dataStreamSupplier = () -> data.stream();
    /**
     * DataSet inner {@link #data} list stream
     */
    private Stream<T> dataStream;
    /**
     * Create with stream parameter constructor need update property {@link #dataStream}
     */
    private volatile boolean streamUpdate = false;
    /**
     * {@link #asyncDataProcessFunction} function temp result
     */
    private volatile List<T> asyncDataFinalValue;
    /**
     * Async data source Future result
     */
    private Future asyncData;
    /**
     * {@link #asyncData} parse Function
     */
    private Function<Object, List<T>> asyncDataProcessFunction;
    /**
     * table name or data source name
     */
    private String source;

    public DataSet() {
    }

    public DataSet(List<T> dataList) {
        Objects.requireNonNull(dataList);
        this.setData(dataList);
    }

    public DataSet(T... dataArray) {
        Objects.requireNonNull(dataArray);
        this.setData(Lists.newArrayList(dataArray));
    }

    public DataSet(Map<? extends FieldNameEnum, Object>... mapRowArray) {
        Objects.requireNonNull(mapRowArray);
        this.setDataStream(Stream.of(mapRowArray).map(eachMapRow -> (T) new Row(eachMapRow)));
    }

    public DataSet(Stream<T> dataStream) {
        this.setDataStream(dataStream);
    }

    /**
     * innerJoin transform operator
     * with default combiner {@link #joinDefaultOneToOneCombiner}
     * with default joinOn Predicate {@link MatchPredicate#equals()}
     *
     * @param rightDs          right DataSet
     * @param leftKeyFunction  left DataSet join key function
     * @param rightKeyFunction right DataSet join key function
     * @param <KL>             left key type
     * @param <KR>             right key type
     * @return new DataSet
     */
    public <KL, KR> DataSet<T> innerJoin(DataSet<T> rightDs,
                                         Function<T, KL> leftKeyFunction,
                                         Function<T, KR> rightKeyFunction) {
        return innerJoin(rightDs, leftKeyFunction, rightKeyFunction, MatchPredicate.equals(),
            DataSet::joinDefaultOneToOneCombiner);
    }

    /**
     * innerJoin transform operator with default combiner {@link #joinDefaultOneToOneCombiner}
     *
     * @param rightDs          right DataSet
     * @param leftKeyFunction  left DataSet join key function
     * @param rightKeyFunction right DataSet join key function
     * @param joinOn           join on predicate
     * @param <KL>             left key type
     * @param <KR>             right key type
     * @return new DataSet
     */
    public <KL, KR> DataSet<T> innerJoin(DataSet<T> rightDs,
                                         Function<T, KL> leftKeyFunction,
                                         Function<T, KR> rightKeyFunction,
                                         BiPredicate<KL, KR> joinOn) {
        return innerJoin(rightDs, leftKeyFunction, rightKeyFunction, joinOn, DataSet::joinDefaultOneToOneCombiner);
    }

    /**
     * innerJoin transform operator
     *
     * @param rightDs          right DataSet
     * @param leftKeyFunction  left DataSet join key function
     * @param rightKeyFunction right DataSet join key function
     * @param joinOn           join on predicate
     * @param combiner         join result combiner
     * @param <KL>             left key type
     * @param <KR>             right key type
     * @return new DataSet
     */
    public <KL, KR> DataSet<T> innerJoin(DataSet<T> rightDs,
                                         Function<T, KL> leftKeyFunction,
                                         Function<T, KR> rightKeyFunction,
                                         BiPredicate<KL, KR> joinOn,
                                         BiFunction<T, T, T> combiner) {
        Stream<T> newStream = SimpleJoinOperator
            .inner(this.getDataStream()).withKey(leftKeyFunction)
            .right(rightDs.getDataStream()).withKey(rightKeyFunction)
            .on(joinOn)
            .combine(combiner)
            .asStream();
        return toDS(newStream);
    }

    /**
     * in transform operator
     *
     * @param rightDs          right DataSet
     * @param leftKeyFunction  left DataSet join key function
     * @param rightKeyFunction right DataSet join key function
     * @param <KL>             left key type
     * @param <KR>             right key type
     * @return new DataSet
     */
    public <KL, KR> DataSet<T> in(DataSet<T> rightDs,
                                  Function<T, KL> leftKeyFunction,
                                  Function<T, KR> rightKeyFunction) {
        int rightSize = rightDs.getData().size();
        Set<KR> rightSet = rightDs.getDataStream().map(e -> rightKeyFunction.apply(e))
            .collect(Collectors.toCollection(() -> new HashSet<>(rightSize)));
        Iterator iterator = Iterators.filter(this.getDataStream().iterator(),
            new CustomInPredicate(rightSet, leftKeyFunction));
        Stream stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
        return new DataSet(stream);
    }

    /**
     * left DataSet item not in right DataSet transform operator
     *
     * @param rightDs          right DataSet
     * @param leftKeyFunction  left DataSet join key function
     * @param rightKeyFunction right DataSet join key function
     * @param <KL>             left key type
     * @param <KR>             right key type
     * @return new DataSet
     */
    public <KL, KR> DataSet<T> notIn(DataSet<T> rightDs,
                                     Function<T, KL> leftKeyFunction,
                                     Function<T, KR> rightKeyFunction) {
        int rightSize = rightDs.getData().size();
        Set<KR> rightSet = rightDs.getDataStream().map(e -> rightKeyFunction.apply(e))
            .collect(Collectors.toCollection(() -> new HashSet<>(rightSize)));
        Iterator iterator = Iterators.filter(this.getDataStream().iterator(),
            Predicates.not(new CustomInPredicate(rightSet, leftKeyFunction)));
        Stream stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
        return new DataSet(stream);
    }

    public <KL, KR> DataSet<T> loopIn(DataSet<T> rightDs,
                                      Function<T, KL> leftKeyFunction,
                                      Function<T, KR> rightKeyFunction) {
        List<T> rightData = rightDs.getData();
        Set<KR> rightSet = new HashSet<>(rightData.size());
        List<T> leftData = this.getData();
        List<T> result = new ArrayList<>(leftData.size());
        for (T row : rightData) {
            rightSet.add(rightKeyFunction.apply(row));
        }
        for (T row : leftData) {
            if (rightSet.contains(leftKeyFunction.apply(row))) {
                result.add(row);
            }
        }
        return new DataSet<>(result);
    }

    public <KL, KR> DataSet<T> loopNotIn(DataSet<T> rightDs,
                                         Function<T, KL> leftKeyFunction,
                                         Function<T, KR> rightKeyFunction) {
        List<T> rightData = rightDs.getData();
        Set<KR> rightSet = new HashSet<>(rightData.size());
        List<T> leftData = this.getData();
        List<T> result = new ArrayList<>(leftData.size());
        for (T row : rightData) {
            rightSet.add(rightKeyFunction.apply(row));
        }
        for (T row : leftData) {
            if (!rightSet.contains(leftKeyFunction.apply(row))) {
                result.add(row);
            }
        }
        return new DataSet<>(result);
    }

    /**
     * innerJoinOneToMany transform operator with default combiner {@link #joinDefaultOneToManyCombiner}
     *
     * @param rightDs          right DataSet
     * @param leftKeyFunction  left DataSet join key function
     * @param rightKeyFunction right DataSet join key function
     * @param joinOn           join on predicate
     * @param <KL>             left key type
     * @param <KR>             right key type
     * @return new DataSet
     */
    public <KL, KR> DataSet<T> innerJoinOneToMany(DataSet<T> rightDs,
                                                  Function<T, KL> leftKeyFunction,
                                                  Function<T, KR> rightKeyFunction,
                                                  BiPredicate<KL, KR> joinOn) {
        return innerJoinOneToMany(rightDs, leftKeyFunction, rightKeyFunction, joinOn,
            DataSet::joinDefaultOneToManyCombiner);
    }

    /**
     * innerJoinOneToMany transform operator
     *
     * @param rightDs          right DataSet
     * @param leftKeyFunction  left DataSet join key function
     * @param rightKeyFunction right DataSet join key function
     * @param joinOn           join on predicate
     * @param combiner         join result combiner
     * @param <KL>             left key type
     * @param <KR>             right key type
     * @return new DataSet
     */
    public <KL, KR> DataSet<T> innerJoinOneToMany(DataSet<T> rightDs,
                                                  Function<T, KL> leftKeyFunction,
                                                  Function<T, KR> rightKeyFunction,
                                                  BiPredicate<KL, KR> joinOn,
                                                  BiFunction<T, Stream<T>, T> combiner) {
        Stream<T> newStream = SimpleJoinOperator
            .inner(this.getDataStream()).withKey(leftKeyFunction)
            .right(rightDs.getDataStream()).withKey(rightKeyFunction)
            .on(joinOn)
            .group(combiner)
            .asStream();
        return toDS(newStream);
    }

    /**
     * leftOuterJoin transform operator with default combiner {@link #joinDefaultOneToOneCombiner}
     *
     * @param rightDs          right DataSet
     * @param leftKeyFunction  left DataSet join key function
     * @param rightKeyFunction right DataSet join key function
     * @param joinOn           join on predicate
     * @param <KL>             left key type
     * @param <KR>             right key type
     * @return new DataSet
     */
    public <KL, KR> DataSet<T> leftOuterJoin(DataSet<T> rightDs,
                                             Function<T, KL> leftKeyFunction,
                                             Function<T, KR> rightKeyFunction,
                                             BiPredicate<KL, KR> joinOn) {
        return leftOuterJoin(rightDs, leftKeyFunction, rightKeyFunction, joinOn, DataSet::joinDefaultOneToOneCombiner);
    }

    /**
     * leftOuterJoin transform operator
     *
     * @param rightDs          right DataSet
     * @param leftKeyFunction  left DataSet join key function
     * @param rightKeyFunction right DataSet join key function
     * @param joinOn           join on predicate
     * @param combiner         join result combiner
     * @param <KL>             left key type
     * @param <KR>             right key type
     * @return new DataSet
     */
    public <KL, KR> DataSet<T> leftOuterJoin(DataSet<T> rightDs,
                                             Function<T, KL> leftKeyFunction,
                                             Function<T, KR> rightKeyFunction,
                                             BiPredicate<KL, KR> joinOn,
                                             BiFunction<T, T, T> combiner) {
        Stream<T> newStream = SimpleJoinOperator
            .leftOuter(this.getDataStream()).withKey(leftKeyFunction)
            .right(rightDs.getDataStream()).withKey(rightKeyFunction)
            .on(joinOn)
            .combine(combiner)
            .asStream();
        return toDS(newStream);
    }

    /**
     * leftOuterJoin transform operator by key equals, performance is better 2x than leftOuterJoin
     *
     * @param rightDs          right DataSet
     * @param leftKeyFunction  left DataSet join key function
     * @param rightKeyFunction right DataSet join key function
     * @param <KL>             left key type
     * @param <KR>             right key type
     * @return new DataSet
     */
    public <KL, KR> DataSet<T> leftOuterJoinOnEquals(DataSet<T> rightDs,
                                                     Function<T, KL> leftKeyFunction,
                                                     Function<T, KR> rightKeyFunction) {
        return leftOuterJoinOnEquals(rightDs, leftKeyFunction, rightKeyFunction, DataSet::joinDefaultOneToOneCombiner);
    }

    /**
     * leftOuterJoin transform operator by key equals, performance is better 2x than leftOuterJoin
     *
     * @param rightDs          right DataSet
     * @param leftKeyFunction  left DataSet join key function
     * @param rightKeyFunction right DataSet join key function
     * @param combiner         join result combiner
     * @param <KL>             left key type
     * @param <KR>             right key type
     * @return new DataSet
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    public <KL, KR> DataSet<T> leftOuterJoinOnEquals(DataSet<T> rightDs,
                                                     Function<T, KL> leftKeyFunction,
                                                     Function<T, KR> rightKeyFunction,
                                                     BiFunction<T, T, T> combiner) {
        int rightSize = rightDs.getData().size();
        Map<KR, T> rightSet = rightDs.getDataStream()
            .collect(Collectors
                .toMap(rightKeyFunction, e -> e, (v1, v2) -> v2, () -> new HashMap<>(rightSize)));
        Stream<T> stream = this.getDataStream()
            .peek(row -> combiner.apply(row, rightSet.get(leftKeyFunction.apply(row))));
        return new DataSet<>(stream);
    }

    /**
     * leftOuterJoinOneToMany transform operator with default combiner {@link #joinDefaultOneToManyCombiner}
     *
     * @param rightDs          right DataSet
     * @param leftKeyFunction  left DataSet join key function
     * @param rightKeyFunction right DataSet join key function
     * @param joinOn           join on predicate
     * @param <KL>             left key type
     * @param <KR>             right key type
     * @return new DataSet
     */
    public <KL, KR> DataSet<T> leftOuterJoinOneToMany(DataSet<T> rightDs,
                                                      Function<T, KL> leftKeyFunction,
                                                      Function<T, KR> rightKeyFunction,
                                                      BiPredicate<KL, KR> joinOn) {
        return leftOuterJoinOneToMany(rightDs, leftKeyFunction, rightKeyFunction, joinOn,
            DataSet::joinDefaultOneToManyCombiner);
    }

    /**
     * leftOuterJoinOneToMany transform operator
     *
     * @param rightDs          right DataSet
     * @param leftKeyFunction  left DataSet join key function
     * @param rightKeyFunction right DataSet join key function
     * @param joinOn           join on predicate
     * @param combiner         join result combiner
     * @param <KL>             left key type
     * @param <KR>             right key type
     * @return new DataSet
     */
    public <KL, KR> DataSet<T> leftOuterJoinOneToMany(DataSet<T> rightDs,
                                                      Function<T, KL> leftKeyFunction,
                                                      Function<T, KR> rightKeyFunction,
                                                      BiPredicate<KL, KR> joinOn,
                                                      BiFunction<T, Stream<T>, T> combiner) {
        Stream<T> newStream = SimpleJoinOperator
            .leftOuter(this.getDataStream()).withKey(leftKeyFunction)
            .right(rightDs.getDataStream()).withKey(rightKeyFunction)
            .on(joinOn)
            .group(combiner)
            .asStream();
        return toDS(newStream);
    }

    /**
     * default join combiner(one to one)
     *
     * @param left  left row
     * @param right right row
     * @param <T>   type extends {@link Row}
     * @return left row reference
     */
    public static <T extends Row> T joinDefaultOneToOneCombiner(T left, T right) {
        if (Objects.nonNull(right)) {
            left.mergeRow(right);
        }
        return left;
    }

    /**
     * default join combiner(one to many)
     *
     * @param left        left row
     * @param rightStream right DataSet stream
     * @param <T>         type extends {@link Row}
     * @return left row reference
     */
    public static <T extends Row> T joinDefaultOneToManyCombiner(T left, Stream<T> rightStream) {
        if (Objects.nonNull(rightStream)) {
            Optional<T> max = rightStream.max((o1, o2) -> ObjectUtils.compare(o1.getScore(), o2.getScore()));
            if (max.isPresent()) {
                return joinDefaultOneToOneCombiner(left, max.get());
            }
        }
        return left;
    }

    /**
     * union transform operator
     *
     * @param otherDataSet other DataSet
     * @return new DataSet
     */
    public DataSet<T> unionAll(DataSet<T> otherDataSet) {
        return isNull(otherDataSet) ? this : toDS(Stream.concat(this.getDataStream(), otherDataSet.getDataStream()));
    }

    /**
     * filter transform operator
     *
     * @param filterPredicate filter predicate
     * @return new DataSet
     */
    public DataSet<T> filter(Predicate<T> filterPredicate) {
        Objects.requireNonNull(filterPredicate);
        return toDS(this.getDataStream().filter(filterPredicate));
    }

    /**
     * cache DataSet to mem storage plugin no ttl
     *
     * @param key       cache key
     * @param cacheType {@link CacheType}
     * @return current DataSet
     */
    public DataSet<T> cache(String key, CacheType cacheType) {
        return cache(key, cacheType, null);
    }

    /**
     * cache DataSet to mem storage plugin with ttl
     *
     * @param key       cache key
     * @param cacheType {@link CacheType}
     * @param ttlMs     ttl milliseconds
     * @return current DataSet
     */
    public DataSet<T> cache(String key, CacheType cacheType, Long ttlMs) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(cacheType);
        CacheUtil.putToCache(key, this.getData(), cacheType, ttlMs);
        return this;
    }

    /**
     * default sort, use stream sort method
     *
     * @return new DataSet
     */
    public DataSet<T> sort() {
        return toDS(this.getDataStream().sorted());
    }

    /**
     * sort with comparator
     *
     * @param comparator sort comparator
     * @return new DataSet
     */
    public DataSet<T> sort(Comparator<T> comparator) {
        Objects.requireNonNull(comparator);
        return toDS(this.getDataStream().sorted(comparator));
    }

    /**
     * sort with score desc
     *
     * @return new DataSet
     */
    public DataSet<T> sortByScoreDesc() {
        return toDS(this.getDataStream().sorted());
    }

    /**
     * read from cache
     *
     * @param key       cache key
     * @param cacheType {@link CacheType}
     * @param <T>       type extends {@link Row}
     * @return
     */
    public static <T extends Row> DataSet<T> readFromCache(String key, CacheType cacheType) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(cacheType);
        List<T> data = CacheUtil.readFromCache(key, cacheType);
        DataSet<T> dataSet = new DataSet<>();
        if (Objects.nonNull(data)) {
            dataSet.setData(data);
        }
        return dataSet;
    }

    /**
     * append AlgInfo to each DataSet item
     *
     * @param key   {@link AlgInfoKey}
     * @param value AlgInfo value
     * @return new DataSet
     */
    public DataSet<T> appendAlgInfo(AlgInfoKey key, Object value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        return toDS(this.getDataStream().peek(row -> row.appendAlgInfo(key, value)));
    }

    public DataSet<T> appendAlgInfoMap(Map<AlgInfoKey, String> algInfoMap) {
        Objects.requireNonNull(algInfoMap);
        if (MapUtils.isNotEmpty(algInfoMap)) {
            return toDS(this.getDataStream().peek(row -> {
                for (Map.Entry<AlgInfoKey, String> entry : algInfoMap.entrySet()) {
                    if (Objects.nonNull(entry.getKey()) && StringUtils.isNotEmpty(entry.getValue())) {
                        row.appendAlgInfo(entry.getKey(), entry.getValue());
                    }
                }
            }));
        } else {
            return this;
        }
    }

    public DataSet<T> appendAlgInfoFromKeyValue(Map<AlgInfoKey, AllFieldName> algInfoKeyMapping) {
        Objects.requireNonNull(algInfoKeyMapping);
        if (MapUtils.isNotEmpty(algInfoKeyMapping)) {
            return toDS(this.getDataStream().peek(row -> {
                for (Map.Entry<AlgInfoKey, AllFieldName> entry : algInfoKeyMapping.entrySet()) {
                    if (Objects.nonNull(entry.getKey()) && Objects.nonNull(entry.getValue())) {
                        row.appendAlgInfo(entry.getKey(), row.getFieldValue(entry.getValue()));
                    }
                }
            }));
        } else {
            return this;
        }
    }

    @SuppressWarnings("unchecked")
    public DataSet<T> fillDefaultValue(Map<AllFieldName, Object> keyValues) {
        Objects.requireNonNull(keyValues);
        if (MapUtils.isNotEmpty(keyValues)) {
            return toDS(this.getDataStream().map(row -> (T) row.putEnumKeyMap(keyValues)));
        } else {
            return this;
        }
    }

    public DataSet<T> fillExtData(Map<String, Object> keyValues) {
        Objects.requireNonNull(keyValues);
        if (MapUtils.isNotEmpty(keyValues)) {
            return toDS(this.getDataStream().peek(row ->
                row.getExtData().putAll(keyValues)
            ));
        } else {
            return this;
        }
    }

    /**
     * DataSet reverse
     *
     * @return new DataSet
     */
    public DataSet<T> reverse() {
        Collections.reverse(this.getData());
        return toDS(this.getData());
    }

    /**
     * random shuffle
     *
     * @return new DataSet
     */
    public DataSet<T> shuffle() {
        FrameCollectionUtils.shuffle(this.getData(), ThreadLocalRandom.current());
        return toDS(this.getData());
    }

    /**
     * default distinct
     *
     * @return new DataSet
     */
    public DataSet<T> distinct() {
        return toDS(this.getDataStream().distinct());
    }

    /**
     * distinct with unique key function
     *
     * @param keyFunction distinct key function
     * @return new DataSet
     */
    public DataSet<T> distinct(Function<T, Object> keyFunction) {
        Objects.requireNonNull(keyFunction);
        Map<Object, T> map = getDataStream().collect(
            Collectors.toMap(keyFunction::apply, e -> e, (v1, v2) -> v1, Maps::newLinkedHashMap));
        return toDS((map.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList())));
    }

    /**
     * DataSet cutoff
     *
     * @param count limit size
     * @return new DataSet
     */
    public DataSet<T> limit(long count) {
        return toDS(this.getDataStream().limit(count));
    }

    /**
     * Find the first item in DataSet
     *
     * @return Optional
     */
    public Optional<T> getFirstItem() {
        return getData().stream().findFirst();
    }

    /**
     * DataSet is empty or not, trigger stream action operator
     *
     * @return boolean
     */
    public boolean isEmpty() {
        return CollectionUtils.isEmpty(this.getData());
    }

    /**
     * DataSet is not empty or not, trigger stream action operator
     *
     * @return boolean
     */
    public boolean isNotEmpty() {
        return CollectionUtils.isNotEmpty(this.getData());
    }

    /**
     * persist DataSet to storage plugin
     *
     * @param key              persist key
     * @param persistConfigKey plugin short class name
     * @return current DataSet
     */
    public DataSet<T> persist(String key, String persistConfigKey) {
        PersistOperator operator = TppObjectFactory.getBean(persistConfigKey, PersistOperator.class);
        return nonNull(operator) ? operator.persist(key, this) : this;
    }

    /**
     * shuffle plugin
     *
     * @param processorContext processor context
     * @param shuffleConfigKey plugin short class name
     * @return current DataSet if plugin is null, else return new DataSet
     */
    public DataSet<T> shuffle(ProcessorContext processorContext, String shuffleConfigKey) {
        ShuffleOperator operator = TppObjectFactory.getBean(shuffleConfigKey, ShuffleOperator.class);
        return nonNull(operator) ? operator.shuffle(processorContext, this) : this;
    }

    /**
     * sort plugin
     *
     * @param processorContext processor context
     * @param sorterConfigKey  plugin short class name
     * @param comparator       sort comparator
     * @return current DataSet if plugin is null, else return new DataSet
     */
    public DataSet<T> sort(ProcessorContext processorContext, String sorterConfigKey,
                           Comparator<? extends Row> comparator) {
        SortOperator operator = TppObjectFactory.getBean(sorterConfigKey, SortOperator.class);
        return nonNull(operator) ? operator.sort(processorContext, this, comparator) : this;
    }

    /**
     * Single DataSet operator
     *
     * @param processorContext
     * @param opConfigKey
     * @return
     */
    public DataSet<T> op(ProcessorContext processorContext, String opConfigKey) {
        SingleDataSetOperator operator = TppObjectFactory.getBean(opConfigKey, SingleDataSetOperator.class);
        return nonNull(operator) ? operator.op(processorContext, this) : this;
    }

    /**
     * Multi DataSet operator
     *
     * @param processorContext
     * @param opConfigKey
     * @param dataSet          other DataSet
     * @return
     */
    public DataSet<T> op(ProcessorContext processorContext, String opConfigKey, DataSet<T> dataSet) {
        MultiDataSetOperator operator = TppObjectFactory.getBean(opConfigKey, MultiDataSetOperator.class);
        return nonNull(operator) ? operator.op(processorContext, this, dataSet) : this;
    }

    public <T extends Row> DataSet<T> map(Function<? super Row, ? extends Row> mapper) {
        return new DataSet(this.getDataStream().map(mapper));
    }

    public <R> Stream<R> maps(Function<? super T, ? extends R> mapper) {
        return this.getDataStream().map(mapper);
    }

    public Stream<T> filters(Predicate<? super T> predicate) {
        return this.getDataStream().filter(predicate);
    }

    public <R, A> R collects(Collector<? super T, A, R> collector) {
        return this.getDataStream().collect(collector);
    }

    public void forEach(Consumer<T> action) {
        this.getDataStream().forEach(action);
    }

    /**
     * create new DataSet operator by data list input
     *
     * @param data data list
     * @return new DataSet
     */
    public static <T extends Row> DataSet<T> toDS(List<T> data) {
        return new DataSet<>(data);
    }

    /**
     * create new DataSet operator by stream input
     *
     * @param rowStream data stream
     * @param <T>       type extends {@link Row}
     * @return new DataSet
     */
    public static <T extends Row> DataSet<T> toDS(Stream<T> rowStream) {
        return new DataSet<>(rowStream);
    }

    /**
     * create new DataSet operator by stream input, trigger stream action operator
     *
     * @param rowStream data stream
     * @return new DataSet
     */
    public static <T extends Row> DataSet<T> actionToNewDS(Stream<T> rowStream) {
        Objects.requireNonNull(rowStream);
        return new DataSet<>(rowStream.collect(Collectors.toList()));
    }

    /**
     * Get the data list
     *
     * @return data list
     */
    public List<T> getData() {
        if (nonNull(asyncData)) {
            return this.getAsyncData();
        }
        this.updateStream();
        return this.data;
    }

    /**
     * get data list stream
     *
     * @return Stream
     */
    protected Stream<T> getDataStream() {
        if (nonNull(asyncData)) {
            if (ThreadLocalUtils.isEnableStreamLazy()) {
                return StreamSupport.stream(() ->
                    Spliterators.spliterator(getAsyncData(), 0), 0, false);
            } else {
                return getAsyncData().stream();
            }
        }
        this.updateStream();
        return (isNull(dataStream) || streamClosed()) ? dataStreamSupplier.get() : dataStream;
    }

    /**
     * dataStream is closed or not
     *
     * @return true:closed, false:not closed
     */
    private boolean streamClosed() {
        boolean closed = false;
        if (nonNull(dataStream) && Objects.nonNull(streamLinkedOrConsumedField)) {
            try {
                closed = (boolean) streamLinkedOrConsumedField.get(dataStream);
            } catch (Exception e) {
            }
        }
        return closed;
    }

    /**
     * process async data
     *
     * @return data list
     */
    private List<T> getAsyncData() {
        if (isNull(this.asyncDataFinalValue)) {
            synchronized (this) {
                if (isNull(asyncDataFinalValue) && nonNull(asyncDataProcessFunction) && nonNull(asyncData)) {
                    this.asyncDataFinalValue = this.asyncDataProcessFunction.apply(this.asyncData);
                    this.asyncDataFinalValue = isNull(asyncDataFinalValue) ? Lists.newArrayList() : asyncDataFinalValue;
                    this.data = this.asyncDataFinalValue;
                    this.asyncData = null;
                }
            }
        }
        return isNull(this.data) ? (this.data = Lists.newArrayList()) : this.data;
    }

    public void setData(List<T> data) {
        Objects.requireNonNull(data);
        this.data = data;
        this.streamUpdate = false;
        this.asyncData = null;
        this.dataStream = null;
    }

    private void setDataStream(Stream<T> dataStream) {
        Objects.requireNonNull(dataStream);
        this.dataStream = dataStream;
        this.streamUpdate = true;
    }

    /**
     * Set async data
     *
     * @param asyncData async service data
     * @param function  async parse function
     */
    public void setAsyncData(Future asyncData, Function<Object, List<T>> function) {
        this.asyncData = asyncData;
        this.asyncDataProcessFunction = function;
    }

    /**
     * DataSet use {@link DataSet#DataSet(Stream)} constructor create
     * {@link #getData()} or {@link #getDataStream()} will trigger stream update
     */
    private void updateStream() {
        if (this.streamUpdate) {
            synchronized (this) {
                if (this.streamUpdate) {
                    this.streamUpdate = false;
                    this.data = dataStream.collect(Collectors.toList());
                    this.dataStream = dataStreamSupplier.get();
                }
            }
        }
    }

    public int size() {
        return getData().size();
    }

    private boolean isNull(Object object) {
        return Objects.isNull(object);
    }

    private boolean nonNull(Object object) {
        return Objects.nonNull(object);
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        Objects.requireNonNull(source);
        this.source = source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (isNull(o) || getClass() != o.getClass()) {
            return false;
        }

        DataSet<?> dataSet = (DataSet<?>) o;

        return nonNull(getData()) ? getData().equals(dataSet.getData()) : isNull(dataSet.getData());
    }

    @Override
    public int hashCode() {
        return nonNull(getData()) ? getData().hashCode() : 0;
    }
}
