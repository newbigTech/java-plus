package org.java.plus.dag.core.base.utils.func;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.em.AlgInfoKey;
import org.java.plus.dag.core.base.em.AlgInfoKeyEnum;
import org.java.plus.dag.core.base.em.FieldNameEnum;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.proc.IDebugDataSample;
import org.java.plus.dag.core.base.utils.EnumUtil;
import org.java.plus.dag.core.base.utils.MapUtil;
import org.java.plus.dag.core.base.utils.StrUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author youku
 */
@SuppressWarnings("all")
public class FuncUtils {
    /**
     *
     */
    public enum FuncType {
        /**
         * tpp context
         */
        ctx("ctx.",
        		s -> createRowFunc(s::toString)),//s -> createCtxFunc(s::toString)),
        /**
         * dataset row
         */
        row(StringUtils.EMPTY,
            s -> createRowFunc(s::toString)),
        /**
         * dataset
         */
        dataSet("DataSet.",
            s -> createDataSetFunc(s::toString)),
        /**
         * processorContext extData
         */
        extData("ctxData.",
            s -> createExtDataFunc(s::toString)),

        /**
         * row algInfoMap
         */
        algInfoMap("algInfoMap.",
            s -> createAlgInfoFunc(s::toString)),
        /**
         * other
         */
        other(StringUtils.EMPTY,
            s -> (Function) createOtherFunc(s::toString)),

        /**
         * processor implements IDebugDataSample
         */
        processor(StringUtils.EMPTY,
            s -> (Function) createProcessorFunc(() -> (IDebugDataSample) s));

        private String prefix;
        private Function<Object, Function<?, Object>> func;

        FuncType(String prefix, Function<Object, Function<?, Object>> func) {
            this.prefix = prefix;
            this.func = func;
        }
    }

    public static List<Pair<FuncType, Function>> createFunc(final String str) {
        return createFunc(str, false);
    }

    public static List<Pair<FuncType, Function>> createFunc(final String str, boolean reserve) {
        return (List) createStr(str, reserve)
            .stream()
            .map(e -> new MutablePair(e.getLeft(), e.getLeft().func.apply(e.getRight())))
            .collect(Collectors.toList());
    }

    public static Function getFuncTypeFunc(FuncType funcType, String str) {
        return funcType.func.apply(str);
    }

    public static List<Pair<FuncType, String>> createStr(final String str) {
        return createStr(str, false);
    }

    public static List<Pair<FuncType, String>> createStr(final String str, boolean reserve) {
        List<Pair<FuncType, String>> result = Lists.newArrayList();
        int start = 0;
        int len = str.length() - 1;
        for (int i = 0; i <= len; i++) {
            int c = str.charAt(i);
            if (c == StringPool.C_DOLLAR) {
                if (reserve && start < i) {
                    result.add(new MutablePair<>(FuncType.other, str.substring(start, i)));
                }
                if (i < len && str.charAt(i + 1) == StringPool.C_LEFT_BRACE) {
                    int end = str.indexOf(StringPool.C_RIGHT_BRACE, i);
                    int beginIndex = i + 2;
                    if (end > beginIndex) {
                        if (StrUtils.startWith(str, FuncType.ctx.prefix, beginIndex, end)) {
                            result.add(new MutablePair<>(FuncType.ctx, str.substring(beginIndex + FuncType.ctx.prefix.length(), end)));
                        } else if (StrUtils.startWith(str, FuncType.extData.prefix, beginIndex, end)) {
                            result.add(new MutablePair<>(FuncType.extData, str.substring(beginIndex + FuncType.extData.prefix.length(), end)));
                        } else if (StrUtils.startWith(str, FuncType.dataSet.prefix, beginIndex, end)) {
                            result.add(new MutablePair<>(FuncType.dataSet, str.substring(beginIndex + FuncType.dataSet.prefix.length(), end)));
                        } else if (StrUtils.startWith(str, FuncType.algInfoMap.prefix, beginIndex, end)) {
                            result.add(new MutablePair<>(FuncType.algInfoMap, str.substring(beginIndex + FuncType.algInfoMap.prefix.length(), end)));
                        } else if (StrUtils.startWith(str, FuncType.row.prefix, beginIndex, end)) {
                            result.add(new MutablePair<>(FuncType.row, str.substring(beginIndex + FuncType.row.prefix.length(), end)));
                        }
                        i = end;
                        start = i + 1;
                    } else {
                        start = i;
                        i = end < 0 ? len : end;
                    }
                } else {
                    start = i;
                }
            }
        }
        if (reserve && start <= len) {
            result.add(new MutablePair<>(FuncType.other, str.substring(start)));
        }
        return result.isEmpty() ? Collections.emptyList() : result;
    }


    public static List<Object> getFuncValue(List<Pair<FuncType, Function>> list, ProcessorContext processorContext, Stream<Row> rows) {
        int clone = list.size() == 1 ? -1 : rows.isParallel() ? 0 : 1;
        Row[] rowArr = clone == -1 ? null : rows.toArray(Row[]::new);
        return ListUtils.emptyIfNull(list).stream().map(e -> getFuncValue(e.getLeft(), e.getRight(), processorContext, clone != -1 ?
            clone == 0 ? Stream.of(rowArr).parallel() : Stream.of(rowArr) : rows))
            .collect(Collectors.toList());
    }


    public static Object getFuncValue(FuncType funcType, Function<Object, Object> func, ProcessorContext
        processorContext, Stream<Row> rows) {
        Object rt = null;
        if (funcType != null && func != null) {
            switch (funcType) {
                case ctx:
                case extData:
                    rt = func.apply(processorContext);
                    break;
                case row:
                case algInfoMap:
                    rt = func.apply(rows.findFirst().orElse(null));
                    break;
                case dataSet:
                    rt = func.apply(rows);
                    break;
                default:
                    rt = func.apply(null);
                    break;
            }
        }
        return rt;
    }

    public static Map<String, List<Pair<FuncType, Function>>> createMapFunc(String params, String firstSplit, String sencondSplit) {
        String[] keyArr = StringUtils.splitByWholeSeparator(StringUtils.trimToNull(params), firstSplit);
        if (ArrayUtils.isNotEmpty(keyArr)) {
            return Arrays.stream(keyArr).map(e -> {
                List<String> arr = StrUtils.split(e, sencondSplit.charAt(0), 2);
                if (arr.size() == 1) {
                    List<Pair<FuncUtils.FuncType, String>> list = FuncUtils.createStr(arr.get(0), true);
                    if (CollectionUtils.isNotEmpty(list)) {
                        return (Map) list.stream().collect(Collectors.toMap(Pair::getRight, p -> Collections.singletonList(new MutablePair<>(p.getLeft(), FuncUtils.getFuncTypeFunc(p.getLeft(), p.getRight()))), (v1, v2) -> v2));
                    }
                } else if (arr.size() >= 2) {
                    return Collections.singletonMap(arr.get(0), FuncUtils.createFunc(arr.get(1), true));
                }
                return null;
            }).filter(Objects::nonNull).reduce(Maps.newLinkedHashMap(), MapUtil::putAll);
        }
        return Collections.emptyMap();
    }

    private static Function<ProcessorContext, Object> createExtDataFunc(Supplier<String> supplier) {
        return processorContext -> processorContext.getContextData().get(supplier.get());
    }

//    private static Function<ProcessorContext, Object> createCtxFunc(Supplier<String> supplier) {
//        return processorContext -> processorContext.getTppContext().getRequestParams(supplier.get());
//    }

    public static Function<ProcessorContext, Boolean> createProcessorFunc(Supplier<IDebugDataSample> supplier) {
        return p -> supplier.get().isValidateToSample(p);
    }

    private static Function<String, String> createOtherFunc(Supplier<String> supplier) {
        return s -> supplier.get();
    }

    private static Function<Stream<Row>, Object> createDataSetFunc(Supplier<String> supplier) {
        return rows -> getDataSetFuncValue(createRowFunc(supplier), rows);
    }

    private static List<Object> getDataSetFuncValue(Function<Row, Object> func, Stream<Row> rows) {
        return rows.map(func).collect(Collectors.toList());
    }

    private static Function<Row, Object> createAlgInfoFunc(Supplier<String> supplier) {
        return r -> r.getAlgInfoMap().get(EnumUtils.getEnum(AlgInfoKey.class, supplier.get()));
    }

    private static Function<Row, Object> createRowFunc(Supplier<String> supplier) {
        Function f = Function.identity();
        if (Objects.isNull(supplier)) {
            return f;
        }
        String str = supplier.get();
        Optional<Enum> optionalEnum = Optional.empty();
        try {
            optionalEnum = Optional.ofNullable(EnumUtil.<Enum>getEnum(str));
        } catch (Exception ignore) {
        }
        String[] arr = StringUtils.split(StringUtils.trimToEmpty(str), StringPool.DOT);
        int start = 0;
        if (optionalEnum.isPresent()) {
            start = 2;
            Optional<Enum> finalOptional = optionalEnum;
            f = r -> r == null ? null : ((Row) r).getFieldValue((FieldNameEnum) finalOptional.get());
        }
        for (int i = start, len = arr.length; i < len; i++) {
            int j = i;
            if (i == 0) {
                f = f.andThen(r -> Optional.ofNullable(EnumUtil.getEnum(arr[j]))
                    .flatMap(t -> Optional.ofNullable(r == null ? null : ((Row) r).getFieldValue((FieldNameEnum) t))).orElse(null));
            } else {
                f = f.andThen(m -> {
                    if (m != null && Map.class.isAssignableFrom(m.getClass()) && !((Map) m).isEmpty()) {
                        Class<?> clazz = ((Map) m).keySet().iterator().next().getClass();
                        if (Enum.class.isAssignableFrom(clazz)) {
                            if (AlgInfoKeyEnum.class.isAssignableFrom(clazz)) {
                                clazz = AlgInfoKey.class;
                            }
                            Enum e = EnumUtils.getEnum((Class) clazz, arr[j]);
                            return e == null ? null : ((Map) m).get(e);
                        } else {
                            return ((Map) m).get(arr[j]);
                        }
                    }
                    return null;
                });
            }
        }
        return f;
    }

    public static List<Pair<FuncType, Function>> addDataSetJoin(List<Pair<FuncType, Function>> list, String join) {
        for (Pair<FuncType, Function> pair : list) {
            if (pair.getLeft() == FuncType.dataSet) {
                pair.setValue(pair.getRight().andThen(obj -> StrUtils.objectToStr(obj, join)));
            }
        }
        return list;
    }
}
