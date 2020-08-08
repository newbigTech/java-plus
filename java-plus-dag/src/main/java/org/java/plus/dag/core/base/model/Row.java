package org.java.plus.dag.core.base.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.java.plus.dag.core.base.em.AlgInfoKeyEnum;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.em.FieldNameEnum;
import org.java.plus.dag.core.base.em.RecallType;
import org.java.plus.dag.core.base.model.AlgInfo.AlgInfoType;
import org.java.plus.dag.core.base.model.AlgInfo.AlgInfoValueType;
import org.java.plus.dag.core.base.utils.Debugger;
import lombok.NonNull;

/**
 * DataSet inner data object
 *
 * @author seven.wxy
 * @date 2018/9/6
 */
@SuppressWarnings("all")
public final class Row implements Serializable, Comparable<Object> {
    private static final long serialVersionUID = -4048997185058424560L;

    public String getId() {
        return getFieldValue(AllFieldName.id);
    }

    public Row setId(String id) {
        setFieldValue(AllFieldName.id, id);
        return this;
    }

    public Integer getType() {
        return getFieldValue(AllFieldName.type);
    }

    public Row setType(Integer type) {
        setFieldValue(AllFieldName.type, type);
        return this;
    }

    public Double getScore() {
        return getFieldValue(AllFieldName.score);
    }

    public Row setScore(Double score) {
        return setFieldValue(AllFieldName.score, score);
    }

    public String getRecExt() {
        return getFieldValue(AllFieldName.recext);
    }

    public Row setRecExt(String recExt) {
        return setFieldValue(AllFieldName.recext, recExt);
    }

    public String getTriggerId() {
        return getFieldValue(AllFieldName.triggerId);
    }

    public Row setTriggerId(String triggerId) {
        return setFieldValue(AllFieldName.triggerId, triggerId);
    }

    public RecallType getRecallType() {
        return getFieldValue(AllFieldName.recallType);
    }

    public Row setRecallType(RecallType recallType) {
        return setFieldValue(AllFieldName.recallType, recallType);
    }

    /**
     * recommend result algInfo map, generate algInfo when result return in solution
     *
     * @return
     */
    public Map<AlgInfoKeyEnum, Object> getAlgInfoMap() {
        return computeIfAbsent(AllFieldName.algInfoMap, (key) -> Maps.newConcurrentMap());
    }

    public Row setAlgInfoMap(Map<AlgInfoKeyEnum, Object> algInfoMap) {
        return setFieldValue(AllFieldName.algInfoMap, algInfoMap);
    }

    public Row newAlgInfoMap() {
        return setAlgInfoMap(Maps.newConcurrentMap());
    }

    /**
     * put algInfo key/value to algInfoMap
     *
     * @param key
     * @param value
     * @return
     */
    public Row appendAlgInfo(AlgInfoKeyEnum key, Object value) {
        boolean allowAppend = key.getType() == AlgInfoType.ON_LINE || (Debugger.isDebug()
            && key.getType() == AlgInfoType.DEBUG);
        if (allowAppend && Objects.nonNull(value)) {
            if (key.getValueType() == AlgInfoValueType.MULTI) {
                ((Set)getAlgInfoMap().computeIfAbsent(key, (k) -> Sets.newLinkedHashSet())).add(value);
            } else {
                getAlgInfoMap().put(key, value);
            }
        }
        return this;
    }

    public <K, V> Map<K, V> getExtData() {
        return computeIfAbsent(AllFieldName.extData, (key) -> Maps.newHashMap());
    }

    public <K, V> Map<K, V> getExtMap() {
        return computeIfAbsent(AllFieldName.extMap, (key) -> Maps.newHashMap());
    }

    public <K, V> Row addExtMap(K key, V v) {
        Map<K, V> map = getExtMap();
        map.put(key, v);
        return this;
    }

    public <K, V> Row addExtData(K key, V v) {
        Map<K, V> map = getExtData();
        map.put(key, v);
        return this;
    }

    public Row setExtData(Map extData) {
        return setFieldValue(AllFieldName.extData, extData);
    }

    private Map<FieldNameEnum, Object> innerData = Maps.newHashMap();

    public <T> Row(Map<? extends FieldNameEnum, T> data) {
        Objects.requireNonNull(data);
        for (Map.Entry<? extends FieldNameEnum, T> entry : data.entrySet()) {
            this.setFieldValue(entry.getKey(), entry.getValue());
        }
    }

    public Row() {
    }

    public Row(Row row) {
        Objects.requireNonNull(row);
        this.innerData = Maps.newHashMap(row.innerData);
    }

    public Row putEnumKeyMap(Map<? extends FieldNameEnum, ?> dataMap) {
        Objects.requireNonNull(dataMap);
        for (Map.Entry<? extends FieldNameEnum, ?> entry : dataMap.entrySet()) {
            if (Objects.nonNull(entry.getKey())) {
                this.setFieldValue(entry.getKey(), entry.getValue());
            }
        }
        return this;
    }

    /**
     * merge to row result to one
     * right row overwrite left row
     *
     * @param row
     */
    public Row mergeRow(Row row) {
        return mergeRow(row, true);
    }

    /**
     * merge to row result to one
     * right row overwrite left row
     *
     * @param row
     */
    public Row mergeRow(Row row, boolean override) {
        if (Objects.nonNull(row)) {
            for (Map.Entry<FieldNameEnum, Object> entry : row.innerData.entrySet()) {
                if (Map.class.isAssignableFrom(entry.getKey().getClazz())) {
                    innerData.merge(entry.getKey(), entry.getValue(), (oldValue, newValue) -> {
                        if (override) {
                            ((Map)oldValue).putAll((Map)newValue);
                        } else {
                            Map nMap = (Map)newValue, oMap = (Map)oldValue;
                            for (Object obj : nMap.keySet()) {
                                oMap.putIfAbsent(obj, nMap.get(obj));
                            }
                        }
                        return oldValue;
                    });
                } else {
                    if (override) {
                        innerData.put(entry.getKey(), entry.getValue());
                    } else {
                        innerData.putIfAbsent(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
        return this;
    }

    public Set<FieldNameEnum> dataKeys() {
        return Collections.unmodifiableSet(innerData.keySet());
    }

    public Map<FieldNameEnum, Object> data() {
        return Collections.unmodifiableMap(innerData);
    }

    /**
     * You must adjust null return value
     *
     * @param fieldName
     * @param <T>
     * @return
     */
    public <T> T getFieldValue(@NonNull FieldNameEnum fieldName) {
        return (T)innerData.get(fieldName);
    }

    public <T> T getFieldValue(@NonNull FieldNameEnum fieldName, T defaultValue) {
        Object result = innerData.get(fieldName);
        return result == null ? defaultValue : (T)result;
    }

    public <T> T getFieldValue(@NonNull FieldNameEnum fieldName, Supplier<T> defaultValue) {
        Object result = innerData.get(fieldName);
        return result == null ? defaultValue.get() : (T)result;
    }

    public <T> T computeIfAbsent(@NonNull FieldNameEnum fieldName, Function<FieldNameEnum, T> mappingFunction) {
        return (T)innerData.computeIfAbsent(fieldName, mappingFunction);
    }

    public Row setFieldValue(@NonNull FieldNameEnum fieldName, final Object value) {
        if (Debugger.isLocal()) {
            try {
                innerData.put(fieldName, fieldName.transfer(value));
            } catch (Exception e) {
                throw new RuntimeException("context[fieldName]" + fieldName, e);
            }
        } else {
            innerData.put(fieldName, fieldName.transfer(value));
        }
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Row row = (Row)o;
        return Objects.equals(innerData, row.innerData);
    }

    @Override
    public int hashCode() {
        return innerData != null ? innerData.hashCode() : 0;
    }

    @Override
    public String toString() {
        return innerData.toString();
    }

    @Override
    public int compareTo(Object o) {
        o = o instanceof Row ? o : null;
        Comparator<Row> comparator = Comparator.comparing(
            (Row key) -> Objects.isNull(key) ? 0D : key.getFieldValue(AllFieldName.score, 0D)).reversed();
        comparator = Comparator.nullsLast(comparator);
        return Objects.compare(this, (Row)o, comparator);
    }

    public Row cloneRow() {
        Row result = new Row();
        result.setId(this.getId());
        result.setScore(this.getScore());
        result.setType(this.getType());
        Map map = (Map)this.innerData.get(AllFieldName.extData);
        result.setExtData(map == null ? null : Maps.newHashMap(map));
        map = (Map)this.innerData.get(AllFieldName.algInfoMap);
        result.setAlgInfoMap(map == null ? null : new ConcurrentHashMap<>(map));
        return result;
    }
}
