package org.java.plus.dag.core.base.utils;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.em.EnumInterface;
import org.java.plus.dag.core.base.em.FieldNameEnum;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.proc.Processor;
import org.java.plus.dag.core.base.utils.tair.TairClient;
//import org.java.plus.dag.core.base.utils.tair.TairUtil;
import org.java.plus.dag.core.ds.DataSourceBE;
import org.java.plus.dag.core.ds.DataSourceIGraph;
import org.java.plus.dag.core.ds.DataSourceTair;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author youku
 */
@SuppressWarnings("all")
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RealDebugger {
    private static final AtomicBoolean INIT = new AtomicBoolean();
    private static Map<? extends Enum, Integer> ENUM_META = Maps.newHashMap();
    @Getter
    private static Set<String> META_KEYS;
    private static final String SKEY_META = "debugger_meta";
    private static final String SKEY_FILE = "debugger_file";
    static final String TRACE_ID = "traceId";
    static final String INPUT = "input";
    static final String OUTPUT = "output";
    static final String COST = "cost";
    static final String EXCEPTION = "exception";
    public static final String DEBUG = "debug";
    public static final String CONTEXT = "context";
    static final String OTHER = "other";
    static final String SPAN_ID = "spanId";
    static final String EVENT_TYPE = "eventType";
    static final String PARENT_ID = "parentId";
    static final String SERVICE_ID = "serviceId";
    static final String UUID = "uuid";
    static final String TIME = "time";
    private static final List<String> META_SKEYS = Lists.newArrayList(SKEY_META, SKEY_FILE);
    private static final Function<Class<? extends Enum>, Field> FIND_CLASSFIELD_FUNC = findSubEnum();
    private static final Joiner.MapJoiner MAP_JOINER = Joiner.on(StringPool.COMMA)
        .withKeyValueSeparator(StringPool.EQUALS)
        .useForNull(StringUtils.EMPTY);
    private static final Joiner JOINER = Joiner.on(StringPool.COMMA).useForNull(StringUtils.EMPTY);
    private static final Gson GSON = new GsonBuilder().serializeNulls().create();
    private static final Map<String, Map<Object, Object>> WRITE_META_MAP = Maps.newHashMap();
    private static Map<Enum, BiFunction<Object, StringBuilder, StringBuilder>> HANDLE_MAP;

    public static void writeEnumMeta() {
        WriteMetaClass.writeMetaMap();
    }

    private static Map<Object, Object> debugTairValue(Map<String, String> meta, Class<? extends Enum> clazz) {
        Map<Object, Object> value = new HashMap<>(2);
        value.put(SKEY_META, MAP_JOINER.join(meta));
        return value;
    }

    private static final Field DUMMY_FIELD = FieldUtils.getField(Void.class, "TYPE", true);

    private static Function<Class<? extends Enum>, Field> findSubEnum() {
        Map<Class<? extends Enum>, Field> cache = Maps.newHashMap();
        return clazz -> {
            if (Objects.nonNull(clazz)) {
                if (!cache.containsKey(clazz)) {
                    int size = 0;
                    Field[] fields = null;
                    try {
                        fields = clazz.getDeclaredFields();
                        size = ObjectUtils.defaultIfNull(fields, ArrayUtils.EMPTY_OBJECT_ARRAY).length;
                    } catch (Exception ignore) {
                    }
                    Field result = DUMMY_FIELD;
                    if (Objects.nonNull(fields)) {
                        for (int i = ArrayUtils.getLength(clazz.getEnumConstants()); i < size; i++) {
                            if (fields[i].getType() == java.lang.Class.class) {
                                result = fields[i];
                                break;
                            }
                        }
                    }
                    cache.put(clazz, result);
                    return result;
                }
                return cache.get(clazz);
            }
            return null;
        };
    }

    private static void doQueryEnum(@NonNull Class<? extends Enum> start, Map<Class<? extends Enum>, String> enumMap) {
        Enum[] enums = Enum.class.isAssignableFrom(start) ? start.getEnumConstants() : null;
        if (Objects.nonNull(enums)) {
            Field subEnumField = FIND_CLASSFIELD_FUNC.apply(start);
            boolean flag = EnumInterface.class.isAssignableFrom(start);
            int sum = 0;
            for (Enum enumValue : enums) {
                if (Objects.nonNull(subEnumField) && subEnumField != DUMMY_FIELD) {
                    try {
                        doQueryEnum((Class<? extends Enum>) FieldUtils.readField(subEnumField, enumValue, true), enumMap);
                    } catch (IllegalAccessException ignore) {
                    }
                }
                sum += flag ? ((EnumInterface) enumValue).getIdentify() : enumValue.name().hashCode();
            }
            enumMap.put(start, String.valueOf(sum));
        }
    }

    public static String inputDataSetDebug(Processor processor, DataSet<Row> inputDataSet) {
        init();
        long begin = System.currentTimeMillis();
        String result = RealDebugger.transformDataSet(inputDataSet);
        Debugger.put(processor, "Debugger_input_useTime", (System.currentTimeMillis() - begin));
        return result;
    }

    private static void init() {
        if (!INIT.getAndSet(true)) {
            DebugInitClass.doInit();
        }
    }

    static String transformDataSet(final DataSet<? extends Row> dataSet) {
        init();
        writeEnumMeta();
        List<FieldNameEnum> allFields = new ArrayList<>((dataSet.getData()
            .parallelStream()
            .flatMap(e -> e.dataKeys().stream())
            .collect(Collectors.toCollection(LinkedHashSet::new))));

        return dataSet.getData()
            .parallelStream()
            .map(e -> transformRow(e, allFields))
            .reduce(new StringBuilder(), (sb, s) -> new StringBuilder(sb).append(s), StringBuilder::append)
            .insert(0, allFields
                .stream()
                .map(e -> StrUtils.trimToString(ENUM_META.get(e)) +
                    (e.getClazz().isEnum() ? StringPool.AT : StringUtils.EMPTY))
                .collect(Collectors.collectingAndThen(
                    Collectors.joining(StringPool.PIPE), (String s) -> StringUtils.isEmpty(s) ? s : s + IOUtils.LINE_SEPARATOR
                )))
            .toString();
    }

    private static <T extends Row> StringBuilder transformRow(T row, List<FieldNameEnum> allFields) {
        int size = allFields.size();
        StringBuilder sb = new StringBuilder(size * 20);
        int index = -1;
        FieldNameEnum allFieldName;
        for (int i = 0; i < size; i++) {
            allFieldName = allFields.get(i);
            Object obj = row.getFieldValue(allFieldName);
            if (Objects.nonNull(obj)) {
                if (index != -1) {
                    if (index == i - 1) {
                        sb.append("|");
                    } else {
                        sb.append("{").append(index).append(",").append(i - 1).append("}|");
                    }
                    index = -1;
                }
                BiFunction<Object, StringBuilder, StringBuilder> biFunction = HANDLE_MAP.get(allFieldName);
                if (biFunction != null) {
                    biFunction.apply(obj, sb).append("|");
                } else {
                    dataToString(obj, allFieldName, sb).append("|");
                }
            } else if (index == -1) {
                index = i;
            }
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.append(IOUtils.LINE_SEPARATOR);
    }

    private static BiFunction<Object, StringBuilder, StringBuilder> createFunc(Enum fieldName) {
        Class<?> clazz = fieldName instanceof FieldNameEnum ? ((FieldNameEnum) fieldName).getClazz() : fieldName.getClass();
        if (Iterable.class.isAssignableFrom(clazz)) {
            return (obj, builder) -> JOINER.appendTo(builder, (Iterable) obj);
        } else if (Map.class.isAssignableFrom(clazz)) {
            return (obj, builder) -> MAP_JOINER.appendTo(builder, (Map) obj);
        } else if (clazz.isArray()) {
            return (obj, builder) -> {
                if (obj instanceof Object[]) {
                    return JOINER.appendTo(builder, (Object[]) obj);
                } else {
                    int size = Array.getLength(obj);
                    for (int i = 0; i < size; i++) {
                        builder.append(Array.get(obj, i)).append(StringPool.COMMA);
                    }
                    if (size > 0) {
                        builder.setLength(builder.length() - 1);
                    }
                    return builder;
                }
            };
        } else if (clazz.isEnum()) {
            return (obj, builder) -> builder.append(ENUM_META.get(obj));
        } else if (CharSequence.class.isAssignableFrom(clazz)) {
            return (obj, builder) -> builder.append((CharSequence) obj);
        } else {
            return (obj, builder) -> builder.append(GSON.toJson(obj));
        }
    }

    private static StringBuilder dataToString(Object obj, FieldNameEnum allFieldName, StringBuilder builder) {
        if (obj != null) {
            Class<?> clazz = allFieldName.getClazz();
            if (Collection.class.isAssignableFrom(clazz)) {
                return JOINER.appendTo(builder, (Iterable) obj);
            } else if (Map.class.isAssignableFrom(clazz)) {
                return MAP_JOINER.appendTo(builder, (Map) obj);
            } else if (clazz.isArray()) {
                if (obj instanceof Object[]) {
                    return JOINER.appendTo(builder, (Object[]) obj);
                } else {
                    int size = Array.getLength(obj);
                    for (int i = 0; i < size; i++) {
                        builder.append(Array.get(obj, i)).append(StringPool.COMMA);
                    }
                    if (size > 0) {
                        builder.setLength(builder.length() - 1);
                    }
                }
            } else if (clazz.isEnum()) {
                builder.append(ENUM_META.get(obj));
            } else if (CharSequence.class.isAssignableFrom(clazz)) {
                builder.append(obj);
            } else {
                builder.append(GSON.toJson(obj));
            }
        }
        return builder;
    }

    /**
     *
     */
    public enum EventType {
        /**
         *
         */
        IGRAPH,
        /**
         *
         */
        BE,
        /**
         *
         */
        PROCESSOR,
        /**
         *
         */
        TAIR
    }

    @NonNull
    static EventType getEventType(Processor processor) {
        if (processor instanceof DataSourceBE) {
            return EventType.BE;
        } else if (processor instanceof DataSourceIGraph) {
            return EventType.IGRAPH;
        } else if (processor instanceof DataSourceTair) {
            return EventType.TAIR;
        } else {
            return EventType.PROCESSOR;
        }
    }

    private static final class WriteMetaClass {
        static {
            if (!WRITE_META_MAP.isEmpty()) {
//                TairClient tairClient = TairUtil.getUnifiedTair();
                for (String key : WRITE_META_MAP.keySet()) {
//                    tairClient.putDatas(key, WRITE_META_MAP.get(key));
                }
                WRITE_META_MAP.clear();
            }
        }

        private static void writeMetaMap() {

        }
    }

    private static final class DebugInitClass {
        static {
            Map<Class<? extends Enum>, String> map = Maps.newHashMap();
            EnumUtil.getENUM_CLASS_CACHE().values().forEach(c -> doQueryEnum(c, map));
            Set<String> metaKeys = new HashSet<>(map.size());
            ProcessorContext processorContext = new ProcessorContext();
//            TairClient tairClient = TairUtil.getUnifiedTair();
//            Map<String, Map<String, Serializable>> tairValue = tairClient.getDataWithMutilPkeySkey(processorContext, map
//                .entrySet()
//                .stream()
//                .collect(Collectors
//                    .toMap((Map.Entry<Class<? extends Enum>, String> e) -> {
//                        String str = e.getKey().getName() + StringPool.DOLLAR + e.getValue();
//                        metaKeys.add(str);
//                        return str;
//                    }, e -> META_SKEYS, (v1, v2) -> v2, Maps::newLinkedHashMap)));
            Map<Enum, Integer> metaEnum = Maps.newHashMap();
            int[] before = {0};
            Map<Class<? extends Enum>, Map<String, String>> tmp = new HashMap<>(map.size());
            map.forEach((k, v) -> {
                Map<String, String> meta = StrUtils.strToMap(ObjectUtils.defaultIfNull(MapUtils
//                        .emptyIfNull(tairValue.get(k.getName() + StringPool.DOLLAR + v))
                		.emptyIfNull((Map<String, String>)new HashMap<String,String>())
                        .get(SKEY_META), StringUtils.EMPTY).toString(),
                    StringPool.COMMA, StringPool.EQUALS);
                tmp.put(k, meta);
                before[0] += meta.size();
            });
            int[] index = {0};
            Map<Enum, BiFunction<Object, StringBuilder, StringBuilder>> handleMap = Maps.newHashMap();
            map.forEach((k, v) -> {
                Map<String, String> meta = tmp.get(k);
                int size = meta.size();
                EnumUtils.getEnumMap(k).forEach((k1, v1) -> {
                    String value = meta.computeIfAbsent((String) k1, k2 -> String.valueOf(before[0] + index[0]++));
                    metaEnum.put((Enum) v1, Integer.valueOf(value));
                    handleMap.put((Enum) v1, createFunc((Enum) v1));
                });
                if (size != meta.size()) {
                    WRITE_META_MAP.put(k.getName() + StringPool.DOLLAR + v, debugTairValue(meta, k));
                }
            });
            ENUM_META = Collections.unmodifiableMap(metaEnum);
            META_KEYS = Collections.unmodifiableSet(metaKeys);
            HANDLE_MAP = Collections.unmodifiableMap(handleMap);
        }

        private static void doInit() {

        }
    }
}
