package org.java.plus.dag.core.base.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.exception.RecException;
import org.java.plus.dag.core.base.exception.StatusType;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.proc.Processor;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonFilter;
import org.codehaus.jackson.map.ser.FilterProvider;
import org.codehaus.jackson.map.ser.impl.SimpleBeanPropertyFilter;
import org.codehaus.jackson.map.ser.impl.SimpleFilterProvider;

/**
 * @author seven.wxy
 * @date 2018/10/10
 */
public class Debugger {
    private static final String FILTER_BY_FIELD_NAME = "FilterByFieldName";

    private static final ThreadLocal<ObjectMapper> OBJECT_MAPPER_LOCAL = ThreadLocal.withInitial(ObjectMapper::new);
    private static final ThreadLocal<PrefixManager> PREFIX_MANAGER = ThreadLocal.withInitial(PrefixManager::new);
    private static final ThreadLocal<Boolean> IS_DEBUG = ThreadLocal.withInitial(Boolean.FALSE::booleanValue);
    private static final ThreadLocal<Boolean> IS_LOCAL = ThreadLocal.withInitial(Boolean.FALSE::booleanValue);
    private static final ThreadLocal<Map<String, Object>> DEBUG_DATA_MAP_LOCAL = ThreadLocal.withInitial(LinkedHashMap::new);
    private static final ThreadLocal<Map<String, String>> DEBUG_PARAM_MAP_LOCAL = new ThreadLocal<>();
    private static final ThreadLocal<List<String>> EXCEPTION_LIST = ThreadLocal.withInitial(ArrayList::new);
    private static final ThreadLocal<Boolean> ENABLE_PROFILER = ThreadLocal.withInitial(Boolean.FALSE::booleanValue);

    public static final String FBK = "fbk";
    public static final String FBV = "fbv";
    public static final String TOP = "top";
    public static final String FBF = "fbf";
    public static final String EBK = "ebk";
    public static final String HOST = "host";
    public static final String UNIT = "unit";
    public static final String SCENE_ID = "sceneId";
    public static final String AB_ID = "abId";
    public static final String SOLUTION_ID = "solutionId";

    public static final String EXP_SIZE = "exception_size";
    public static final String EXP = "exception";
    public static final String COST = "cost";
    public static final String SIZE = "size";
    public static final String END = "end";
    public static final String CFG = "cfg";
    public static final String RTP = "rtp";
    public static final String THREAD = "thread";
    public static final String TIME = "time";
    public static final String LOCAL = "local";

    public static boolean isDebug() {
        return IS_DEBUG.get();
    }

    public static boolean isLocal() {
        return IS_LOCAL.get();
    }

    public static boolean isEnableProfiler() {
        return ENABLE_PROFILER.get();
    }

    public static void setDebug(boolean debug) {
        IS_DEBUG.set(debug);
    }

    public static void setEnableProfiler(boolean enableProfiler) {
        ENABLE_PROFILER.set(enableProfiler);
    }

    public static void setLocal(boolean local) {
        IS_LOCAL.set(local);
    }

    public static void setExceptionList(List exceptionList) {
        EXCEPTION_LIST.set(exceptionList);
    }

    public static void init(ProcessorContext context) {
        if (context.getDebug()) {
            Logger.initDebugLog();
            Debugger.setDebug(true);
            Debugger.setDebugParamMap(StrUtils.strToMap(context.getDebugParam(), StringPool.COMMA, StringPool.COLON));
            Debugger.setDebugDataMap(Maps.newLinkedHashMap());
            Debugger.setExceptionList(Lists.newArrayList());
        }
        if (Objects.equals(LOCAL, context.getEnv())) {
            Debugger.setLocal(true);
        }
        Debugger.setEnableProfiler(context.isEnableProfiler());
    }

    /**
     * run at last, clear ThreadLocal
     */
    public static void clear() {
        OBJECT_MAPPER_LOCAL.remove();
        PREFIX_MANAGER.remove();
        IS_DEBUG.remove();
        IS_LOCAL.remove();
        DEBUG_DATA_MAP_LOCAL.remove();
        DEBUG_PARAM_MAP_LOCAL.remove();
        EXCEPTION_LIST.remove();
        ENABLE_PROFILER.remove();
    }

    public static void put(Object thisObj, Supplier<String> keySupplier, Supplier<Object> valueSupplier) {
        String key;
        Object value;
        if (isDebug() && Objects.nonNull(thisObj) && StringUtils.isNotEmpty(key = keySupplier.get()) && Objects.nonNull(value = valueSupplier.get())) {
            _doPut(thisObj, key, value);
        }
    }

    public static void put(Object thisObj, String key, Supplier<Object> supplier) {
        Object value;
        if (isDebug() && Objects.nonNull(thisObj) && Objects.nonNull(key) && Objects.nonNull(value = supplier.get())) {
            _doPut(thisObj, key, value);
        }
    }

    public static boolean containsCost() {
        boolean result = false;
        Map<String, String> paramMap = DEBUG_PARAM_MAP_LOCAL.get();
        if (MapUtils.isNotEmpty(paramMap)) {
            result = StrUtils.strToStrSet(paramMap.get(FBK), StringPool.UNDERSCORE).contains(COST);
        }
        return result;
    }

    private static void _doPut(Object thisObj, String key, Object value) {
        try {
            Set<String> fbk = Collections.emptySet();
            Set<String> fbv = Collections.emptySet();
            Set<String> fbf = Collections.emptySet();
            Set<String> ebk = Collections.emptySet();
            int top = -1;

            Map<String, String> paramMap = DEBUG_PARAM_MAP_LOCAL.get();
            if (MapUtils.isNotEmpty(paramMap)) {
                fbk = StrUtils.strToStrSet(paramMap.get(FBK), StringPool.UNDERSCORE);
                fbv = StrUtils.strToStrSet(paramMap.get(FBV), StringPool.UNDERSCORE);
                fbf = StrUtils.strToStrSet(paramMap.get(FBF), StringPool.UNDERSCORE);
                ebk = StrUtils.strToStrSet(paramMap.get(EBK), StringPool.UNDERSCORE);
                String topStr = paramMap.get(TOP);
                if (StringUtils.isNotBlank(topStr)) {
                    top = Integer.valueOf(topStr);
                }
            }
            String realKey = PREFIX_MANAGER.get().getPrefix(thisObj, key);
            if ((ebk.size() > 0 && isLike(fbk, realKey) && !isLike(ebk, realKey))
                    || (ebk.size() == 0 && isLike(fbk, realKey))) {
                Object filterValue = getFilterValue(fbv, value, top);
                if (Objects.nonNull(filterValue) && StringUtils.isNotEmpty(filterValue.toString())) {
                    String formatValue;
                    if (fbf.isEmpty()) {
                        ObjectMapper om = OBJECT_MAPPER_LOCAL.get();
                        formatValue = om.writerWithDefaultPrettyPrinter().writeValueAsString(filterValue);
                    } else {
                        // only output fbf fields
                        ObjectMapper om = OBJECT_MAPPER_LOCAL.get();
                        om.getSerializationConfig().addMixInAnnotations(Object.class, PropertyFilterMixIn.class);
                        FilterProvider filters = new SimpleFilterProvider().addFilter(FILTER_BY_FIELD_NAME,
                                SimpleBeanPropertyFilter.filterOutAllExcept(fbf));
                        formatValue = om.writer(filters).withDefaultPrettyPrinter().writeValueAsString(filterValue);
                    }
                    formatValue = StringUtils.removeStart(formatValue, StringPool.QUOTE);
                    formatValue = StringUtils.removeEnd(formatValue, StringPool.QUOTE);
                    DEBUG_DATA_MAP_LOCAL.get().put(realKey, formatValue);
                }
            }
        } catch (Exception ex) {
            //ignore
        }
    }

    /**
     * put debug info to current thread info map
     *
     * @param thisObj current object or class object
     * @param key
     * @param value
     */
    public static void put(Object thisObj, String key, Object value) {
        if (isDebug() && Objects.nonNull(thisObj) && Objects.nonNull(key) && Objects.nonNull(value)) {
            _doPut(thisObj, key, value);
        }
    }

    /**
     * output all debug info when debugParam is null, so wordSet.size() = 0, isLike = true
     *
     * @param wordSet
     * @param realKey
     * @return
     */
    private static boolean isLike(Set<String> wordSet, String realKey) {
        boolean isLike = false;
        if (wordSet.size() > 0) {
            for (String searchKey : wordSet) {
                if (StringUtils.containsIgnoreCase(realKey, searchKey)) {
                    isLike = true;
                    break;
                }
            }
        } else {
            isLike = true;
        }
        return isLike;
    }

    /**
     * getFilterValue
     *
     * @param wordSet
     * @param obj
     * @param top
     * @return
     */
    private static Object getFilterValue(Set<String> wordSet, Object obj, int top) {
        Object returnObj = null;
        if (null != obj) {
            if (obj instanceof List) {
                List result = new ArrayList();
                for (Object o : (List) obj) {
                    if (isLike(wordSet, o.toString())) {
                        result.add(o);
                    }
                }
                List subList = result;
                if (top > 0) {
                    subList = result.subList(0, Math.min(top, result.size()));
                }
                if (subList.size() > 0) {
                    returnObj = subList;
                } else {
                    returnObj = Lists.newArrayList();
                }
            } else {
                if (isLike(wordSet, obj.toString())) {
                    returnObj = obj;
                }
            }
        }
        return returnObj;
    }

    @JsonFilter(FILTER_BY_FIELD_NAME)
    static class PropertyFilterMixIn {
    }

    static class PrefixManager {
        public String getPrefix(Object obj, String key) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
            String className;
            if (obj instanceof Processor) {
                className = ((Processor) obj).getInstanceKey();
            } else {
                Class cls = obj.getClass();
                String clazz = "Class";
                if (!clazz.equals(cls.getSimpleName())) {
                    className = BeanUtils.getSimpleProperty(cls, "name");
                } else {
                    className = BeanUtils.getSimpleProperty(obj, "name");
                }
                className = StringUtils.replace(className, ConstantsFrame.PACKAGE_NAME, StringUtils.EMPTY);
            }
            StringBuilder prefixSb = new StringBuilder();
            prefixSb.append(className)
                    .append(StringPool.HASH).append(System.nanoTime() % 1_000_000_000)
                    .append(StringPool.UNDERSCORE).append(key);
            return prefixSb.toString();
        }
    }

    public static Map<String, Object> getDebugDataMap() {
        return DEBUG_DATA_MAP_LOCAL.get();
    }

    public static void setDebugParamMap(Map<String, String> debugParam) {
        DEBUG_PARAM_MAP_LOCAL.set(debugParam);
    }

    public static void setDebugDataMap(Map<String, Object> debugData) {
        DEBUG_DATA_MAP_LOCAL.set(debugData);
    }

    public static boolean isRtpDebug() {
        boolean isRtpDebug = false;
        if (DEBUG_PARAM_MAP_LOCAL.get() != null && IS_DEBUG.get()) {
            String rtpValue = DEBUG_PARAM_MAP_LOCAL.get().getOrDefault(RTP, "0");
            isRtpDebug = "1".equals(rtpValue);
        }
        return isRtpDebug;
    }

    public static boolean viewThread() {
        boolean viewThread = false;
        if (DEBUG_PARAM_MAP_LOCAL.get() != null && IS_DEBUG.get()) {
            String value = DEBUG_PARAM_MAP_LOCAL.get().getOrDefault(THREAD, "0");
            viewThread = "1".equals(value);
        }
        return viewThread;
    }

    public static boolean viewTime() {
        boolean viewTime = false;
        if (DEBUG_PARAM_MAP_LOCAL.get() != null && IS_DEBUG.get()) {
            String value = DEBUG_PARAM_MAP_LOCAL.get().getOrDefault(TIME, "0");
            viewTime = "1".equals(value);
        }
        return viewTime;
    }

    /**
     * put exception to thread local list
     *
     * @param thisObj    current object or class object
     * @param statusType exception enum StatusType
     * @param t          Exception
     */
    public static void exception(Object thisObj, StatusType statusType, Throwable t) {
        if (isDebug()) {
            EXCEPTION_LIST.get().add("code:" + statusType.getStatus() + ",msg:" + statusType.getMsg() + "," +  ExceptionUtils.getStackTrace(t));
        }
        Logger.onlineWarn(() -> String.format("Exp instKey:%s msg:%s",
            ((thisObj instanceof Processor) ? ((Processor)thisObj).getInstanceKey() : thisObj.getClass().getName()),
            ExceptionUtils.getStackTrace(t)));
    }

    public static void exception(Object thisObj, int code, String message, Throwable t) {
        if (isDebug()) {
            EXCEPTION_LIST.get().add("code:" + code + ",msg:" + message + "," +  ExceptionUtils.getStackTrace(t));
        }
        Logger.onlineWarn(() -> String.format("Exp instKey:%s msg:%s exp:%s",
            ((thisObj instanceof Processor) ? ((Processor)thisObj).getInstanceKey() : thisObj.getClass().getName()),
            message,
            ExceptionUtils.getStackTrace(t)));
    }

    public static List<String> getExceptionList() {
        return EXCEPTION_LIST.get();
    }
}
