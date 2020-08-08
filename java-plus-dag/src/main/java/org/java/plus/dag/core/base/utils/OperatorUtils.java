package org.java.plus.dag.core.base.utils;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSONObject;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
//import com.taobao.recommendplatform.protocol.solution.Context;
import org.java.plus.dag.core.base.annotation.ConfigInit;
import org.java.plus.dag.core.base.cache.CacheType;
import org.java.plus.dag.core.base.cache.CacheUtil;
import org.java.plus.dag.core.base.cache.LruCacheConfig;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.exception.StatusType;
import org.java.plus.dag.core.base.meta.OperatorAttribute;
import org.java.plus.dag.core.base.meta.OperatorMeta;
import org.java.plus.dag.core.base.proc.AbstractBaseProcessor;
import org.java.plus.dag.core.base.proc.AbstractInit;
import org.java.plus.dag.core.base.proc.AbstractProcessor;
import org.java.plus.dag.core.base.utils.PackageUtils.ClassFilter;
import org.java.plus.dag.solution.Context;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

/**
 * Operator class scan utils
 *
 * @author seven.wxy
 * @date 2019/9/6
 */
public class OperatorUtils {
    private static final String OPERATOR_META = "operator_meta";
    private static final String INVOKE_TYPE = "invoke_type";

    private static final String NAME = "name";
    private static final String NAME_CN = "nameCn";
    private static final String DESC = "desc";
    private static final String REQUIRED = "required";
    private static final String MIN = "min";
    private static final String MAX = "max";
    private static final String DEFAULT_VALUE = "defaultValue";
    private static final String TYPE = "type";
    private static final String STRING = "string";
    private static final String OBJECT = "object";

    private static final String CONFIG_PREFIX = "����ǰ׺";
    private static final String CONFIG_SUFFIX = "���ú�׺";
    private static final String PREFIX = "__prefix__";
    private static final String SUFFIX = "__suffix__";

    private static final Splitter SLASH_SPLITTER = Splitter.on(StringPool.SLASH);

    private static final String[] SCAN_PACKAGES = new String[] {
        "org.java.plus.dag.biz",
        "org.java.plus.dag.service",
        "org.java.plus.dag.datasource",
        "org.java.plus.dag.engine.dag"
    };

    private static final ClassFilter CLASS_FILTER = new ClassFilter() {
        @Override
        public boolean preFilter(String className) {
            String[] excludes = new String[] {".biz.base.", ".biz.demo", ".utils.", ".model.", "$", ".service.dev.",
                ".other.PlayHistory", ".other.PlayLogPo", ".other.OnlineCacheBase",
                ".other.FeedbackReason", ".strategy.CommonSplitStrategy", ".datasource.BEDataSource",
                ".strategy.BaseSplitStrategy"
            };
            return StringUtils.indexOfAny(className, excludes) == -1;
        }

        @Override
        public boolean filter(Class clazz) {
            return true;
        }
    };

    private static final String CACHE_INSTANCE_KEY = "ALL_OPERATORS_CACHE";
    private static final long CACHE_TIME_OUT = 5 * 60;

    public static boolean isOperatorMeta(Context context) {
        return Objects.equals(context.getRequestParams(INVOKE_TYPE), OPERATOR_META);
    }

    public static List getOperatorMeta(Context context) {
        String key = CACHE_INSTANCE_KEY + context.getCurrentAppId();
        Map<String, List> res = CacheUtil.readFromCache(CACHE_INSTANCE_KEY,
            Lists.newArrayList(key),
            CacheType.HEAP, () -> new LruCacheConfig<>(1000, CACHE_TIME_OUT, keys -> {
                List list = generatePlatformOperatorMeta(generateOperatorMeta(OperatorUtils.getAllOperators()));
                if (CollectionUtils.isEmpty(list)) {
                    return Maps.newHashMap();
                }
                return (Map) Maps.asMap(Sets.newHashSet(keys), k -> list);
            }));
        return res.get(key);
    }

    public static Map<String, Class<?>> scanPackages(String... scanPackages) {
        Map<String, Class<?>> result = PackageUtils.findClassesInPackage(CLASS_FILTER, scanPackages);
        Map<String, Class<?>> convertResult = result.entrySet()
            .stream().collect(Collectors.toMap(
                e -> StringUtils.replace(
                    StringUtils.replace(e.getValue().getName(), ConstantsFrame.PACKAGE_NAME, StringUtils.EMPTY),
                    StringPool.DOT, StringPool.SLASH),
                e -> e.getValue(),
                (v1, v2) -> v2));
        return convertResult;
    }

    public static Map<String, Class<?>> getAllOperators() {
        return OperatorUtils.scanPackages(SCAN_PACKAGES);
    }

    public static Map<String, List<Map<String, Object>>> generateOperatorMeta(Map<String, Class<?>> operatorClassMap) {
        Map<String, List<Map<String, Object>>> result = Maps.newConcurrentMap();
        operatorClassMap.forEach((key, value) -> {
            List<Field> allFieldsList = FieldUtils.getAllFieldsList(value);
            List<Map<String, Object>> fields = Lists.newArrayList();
            for (Field field : allFieldsList) {
                ConfigInit annotation = field.getAnnotation(ConfigInit.class);
                if (Objects.isNull(annotation)) {
                    continue;
                }
                if (Objects.equals(field.getDeclaringClass().getName(), AbstractProcessor.class.getName())
                    || Objects.equals(field.getDeclaringClass().getName(), AbstractBaseProcessor.class.getName())
                    || Objects.equals(field.getDeclaringClass().getName(), AbstractInit.class.getName())) {
                    continue;
                }
                Object instance = newInstance(value);
                if (Objects.isNull(instance)) {
                    continue;
                }
                fields.add(parseFieldMap(field, annotation, instance));
            }
            result.put(key, fields);
        });
        return result;
    }

    private static Object newInstance(Class clazz) {
        try {
            return clazz.newInstance();
        } catch (Exception e) {
            Debugger.exception(OperatorUtils.class, StatusType.CONFIG_INIT_EXCEPTION, e);
            return null;
        }
    }

    private static Map<String, Object> parseFieldMap(Field field, ConfigInit annotation, Object instance) {
        Map<String, Object> fieldMap = Maps.newHashMap();
        String name = StringUtils.isEmpty(annotation.name()) ? field.getName() : annotation.name();
        fieldMap.put(NAME, name);
        fieldMap.put(NAME_CN, StringUtils.isEmpty(annotation.nameCn()) ? name : annotation.nameCn());
        fieldMap.put(DESC, annotation.desc());
        if (annotation.required()) {
            fieldMap.put(REQUIRED, annotation.required());
        }
        if (annotation.min() != Long.MIN_VALUE) {
            fieldMap.put(MIN, annotation.min());
        }
        if (annotation.max() != Long.MAX_VALUE) {
            fieldMap.put(MAX, annotation.max());
        }
        fieldMap.put(DEFAULT_VALUE, InitUtils.getFieldValue(instance, field, annotation, null));
        String typeName = field.getGenericType().getTypeName();
        String convertType = String.class.getSimpleName().toLowerCase();
        if (Objects.equals(typeName, Integer.class.getName()) || Objects.equals(typeName, int.class.getName())) {
            convertType = Integer.class.getSimpleName().toLowerCase();
        } else if (Objects.equals(typeName, Long.class.getName()) || Objects.equals(typeName, long.class.getName())) {
            convertType = Long.class.getSimpleName().toLowerCase();
        } else if (Objects.equals(typeName, Double.class.getName()) || Objects.equals(typeName,
            double.class.getName())) {
            convertType = Double.class.getSimpleName().toLowerCase();
        } else if (Objects.equals(typeName, Boolean.class.getName()) || Objects.equals(typeName,
            boolean.class.getName())) {
            convertType = Boolean.class.getSimpleName().toLowerCase();
        }
        fieldMap.put(TYPE, convertType);
        return fieldMap;
    }

    public static List generatePlatformOperatorMeta(Map<String, List<Map<String, Object>>> operatorMeta) {
        List result = Lists.newArrayListWithCapacity(operatorMeta.size());
        operatorMeta.forEach((key, value) -> {
            List<String> keySplit = SLASH_SPLITTER.splitToList(key);
            List<String> tags = Lists.newArrayList();
            String className = key;
            int size = keySplit.size();
            if (size > 0) {
                for (int i = 1; i < size; i++) {
                    if (i == size - 1) {
                        className = keySplit.get(i);
                    } else {
                        tags.add(keySplit.get(i));
                    }
                }
            }
            Map<String, Object> properties = Maps.newHashMap();
            OperatorAttribute jsonConfig = new OperatorAttribute();
            Map jsonProp = Maps.newLinkedHashMap();
            List<String> requiredList = Lists.newArrayList();
            for (Map<String, Object> map : value) {
                String name = String.valueOf(map.get(NAME));
                OperatorAttribute v = new OperatorAttribute();
                v.setDescription(String.valueOf(map.get(NAME_CN)));
                v.setTip(StringUtils.defaultIfEmpty(String.valueOf(map.get(DESC)), String.valueOf(map.get(NAME_CN))));
                v.setType(String.valueOf(map.get(TYPE)));
                v.setDefaultValue(map.get(DEFAULT_VALUE));
                Object required = map.get(REQUIRED);
                if (Objects.nonNull(required)) {
                    requiredList.add(name);
                }
                Object min = map.get(MIN);
                if (Objects.nonNull(min)) {
                    v.setMinimum((Long)min);
                }
                Object max = map.get(MAX);
                if (Objects.nonNull(max)) {
                    v.setMaximum((Long)max);
                }
                jsonProp.put(name, v);
            }

            OperatorAttribute prefix = new OperatorAttribute();
            prefix.setDescription(CONFIG_PREFIX);
            prefix.setTip(CONFIG_PREFIX);
            prefix.setType(STRING);
            prefix.setDefaultValue(StringUtils.EMPTY);
            jsonProp.put(PREFIX, prefix);
            requiredList.add(CONFIG_PREFIX);

            OperatorAttribute suffix = new OperatorAttribute();
            suffix.setDescription(CONFIG_SUFFIX);
            suffix.setTip(CONFIG_SUFFIX);
            suffix.setType(STRING);
            suffix.setDefaultValue(StringUtils.EMPTY);
            jsonProp.put(SUFFIX, suffix);

            jsonConfig.setDescription(className);
            jsonConfig.setType(OBJECT);
            jsonConfig.setProperties(jsonProp);
            jsonConfig.setRequired(requiredList);
            OperatorAttribute valueMap = new OperatorAttribute();
            valueMap.setDescription(className);
            valueMap.setType(STRING);
            valueMap.setJsonConfig(jsonConfig);
            properties.put(className, valueMap);
            OperatorAttribute attrs = new OperatorAttribute();
            attrs.setName(className);
            attrs.setDescription(className);
            attrs.setType(OBJECT);
            attrs.setProperties(properties);
            OperatorMeta meta = new OperatorMeta();
            meta.setName(key)
                .setDescription(className)
                .setTip(key)
                .setTags(tags)
                .setAttrs(attrs);
            result.add(meta);
        });
        return result;
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        Map<String, Class<?>> result = OperatorUtils.getAllOperators();
        System.out.println("Class count:" + result.size() + "," + JSONObject.toJSONString(result));
        System.out.println(System.currentTimeMillis() - start);
        Map<String, List<Map<String, Object>>> fieldMap = generateOperatorMeta(result);
        System.out.println("Class field count:" + fieldMap.size() + "," + JSONObject.toJSONString(fieldMap));
        System.out.println(System.currentTimeMillis() - start);
        System.out.println(new Gson().toJson(generatePlatformOperatorMeta(fieldMap)));
    }
}
