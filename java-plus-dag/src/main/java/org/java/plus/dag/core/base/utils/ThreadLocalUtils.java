package org.java.plus.dag.core.base.utils;

//import com.alibaba.common.lang.diagnostic.Profiler;
//import com.alibaba.common.lang.diagnostic.Profiler.Entry;
import com.alibaba.fastjson.JSONObject;
//import com.alibaba.glaucus.common.ThreadLocalParams;
//import com.taobao.eagleeye.EagleEye;
//import com.taobao.recommendplatform.protocol.solution.Context;
//import com.taobao.recommendplatform.protocol.solution.TppHyperspaceResult;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.solution.Context;

import lombok.Getter;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author seven.wxy
 * @date 2018/12/29
 */
@SuppressWarnings("unchecked")
public class ThreadLocalUtils {
    public static Field entryStackField;
    public static Field subEntriesField;
    public static Method getCurrentEntryMethod;
    public static Method entryReleaseMethod;

    private static final String TB_EAGLE_EYE_T = "tb_eagleeyex_t";
    private static final String PRESSURE_TEST_VALUE_T = "t";
    private static final String PRESSURE_TEST_VALUE_1 = "1";
    private static final String PRESSURE_TEST_VALUE_2 = "2";

    static {
        try {
//            entryStackField = Profiler.class.getDeclaredField("entryStack");
//            entryStackField.setAccessible(true);
//            subEntriesField = Profiler.Entry.class.getDeclaredField("subEntries");
//            subEntriesField.setAccessible(true);
//            getCurrentEntryMethod = Profiler.class.getDeclaredMethod("getCurrentEntry");
//            getCurrentEntryMethod.setAccessible(true);
//            entryReleaseMethod = Profiler.Entry.class.getDeclaredMethod("release");
//            entryReleaseMethod.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException("Profiler reflect error", e);
        }
    }

    private static final ThreadLocal<Context> CONTEXT = new ThreadLocal<>();
    private static final ThreadLocal<Boolean> ENABLE_TPP_LAYER = ThreadLocal.withInitial(Boolean.FALSE::booleanValue);
    private static final ThreadLocal<Boolean> ENABLE_MANUAL_LAYER = ThreadLocal.withInitial(Boolean.FALSE::booleanValue);
    private static final ThreadLocal<Boolean> ENABLE_STREAM_LAZY = ThreadLocal.withInitial(Boolean.TRUE::booleanValue);
    private static final ThreadLocal<Boolean> WRITE_I_GRAPH_RT_TO_TT = ThreadLocal.withInitial(Boolean.FALSE::booleanValue);
    private static final ThreadLocal<Boolean> WRITE_BE_RT_TO_TT = ThreadLocal.withInitial(Boolean.FALSE::booleanValue);
    private static final ThreadLocal<Boolean> ASYNC_IGRAPH = ThreadLocal.withInitial(Boolean.TRUE::booleanValue);
    private static final ThreadLocal<Integer> ASYNC_IGRAPH_TIME_OUT = ThreadLocal.withInitial(() -> 50);
    private static final ThreadLocal<Boolean> MULTI_THREAD_EXECUTE = ThreadLocal.withInitial(Boolean.TRUE::booleanValue);
    private static final ThreadLocal<Boolean> USE_TPP_THREAD_POOL = ThreadLocal.withInitial(Boolean.TRUE::booleanValue);
    private static final ThreadLocal<Boolean> USE_NEW_WISP_THREAD_POOL = ThreadLocal.withInitial(Boolean.FALSE::booleanValue);
    private static final ThreadLocal<Boolean> OFF_RANDOM = ThreadLocal.withInitial(Boolean.FALSE::booleanValue);
    private static final ThreadLocal<JSONObject> MOCK_TPP_CONFIG = new ThreadLocal<>();
    private static final ThreadLocal<String> EXECUTE_SOLUTION = ThreadLocal.withInitial(() -> SolutionConfig.THREAD);

//    private static final ThreadLocal<TppHyperspaceResult> MANUAL_HYPERSPACE_RESULT = new ThreadLocal<>();
    private static final ThreadLocal<Map<String, JSONObject>> MANUAL_HYPERSPACE_PARAMS_RESULT = ThreadLocal.withInitial(HashMap::new);
    private static final ThreadLocal<Map<String, JSONObject>> TPP_HYPERSPACE_PARAMS_JSON_RESULT = ThreadLocal.withInitial(HashMap::new);
    private static final ThreadLocal<Map<String, String>> TPP_HYPERSPACE_PARAMS_RESULT = ThreadLocal.withInitial(HashMap::new);
    @Getter
    private static final ThreadLocal<Boolean> TPP_DEBUG_SAMPLE = new ThreadLocal<>();

    private static final Map<String, ThreadLocal> FIELDS = new HashMap<>();

    static {
        Field[] fields = ThreadLocalUtils.class.getFields();
        for (Field field : fields) {
            field.setAccessible(true);
            try {
                Object o = field.get(null);
                if (o instanceof ThreadLocal) {
                    FIELDS.put(field.getName(), (ThreadLocal)o);
                }
            } catch (IllegalAccessException ignore) {
            }
        }
    }

//    public static void setEntryStack(Entry entry) {
//        if (Objects.nonNull(entryStackField) && Objects.nonNull(entry)) {
//            try {
//                ThreadLocal threadLocal = ((ThreadLocal) FieldUtils.readStaticField(entryStackField));
//                if (Objects.nonNull(threadLocal)) {
//                    threadLocal.set(entry);
//                }
//            } catch (Exception e) {
//            }
//        }
//    }

//    public static Profiler.Entry getCurrentEntry() {
//        Profiler.Entry result = null;
//        if (Objects.nonNull(getCurrentEntryMethod)) {
//            try {
//                result = (Profiler.Entry) getCurrentEntryMethod.invoke(null);
//            } catch (Exception e) {
//            }
//        }
//        return result;
//    }

//    public static void releaseEntry(Profiler.Entry entry) {
//        if (Objects.nonNull(entry) && Objects.nonNull(entryReleaseMethod)) {
//            try {
//                entryReleaseMethod.invoke(entry);
//            } catch (Exception ignored) {
//            }
//        }
//    }

//    public static void appendToMainEntry(Profiler.Entry mainEntry, Profiler.Entry entry) {
//        if (Objects.nonNull(subEntriesField)
//            && Objects.nonNull(mainEntry) && Objects.nonNull(entry)
//            && !StringUtils.equals(mainEntry.getMessage(), entry.getMessage())) {
//            try {
//                List subEntries = (List) subEntriesField.get(mainEntry);
//                if (Objects.nonNull(subEntries)) {
//                    subEntries.add(entry);
//                }
//            } catch (Exception e) {
//            }
//        }
//    }

    public static boolean isPressureTest() {
        boolean result = false;
        Context context = ThreadLocalUtils.getContext();
        if (Objects.nonNull(context)) {
            Object tbEagleEyeXt = context.get(TB_EAGLE_EYE_T);
            Object t = context.get(PRESSURE_TEST_VALUE_T);
            boolean isTbEagleEyeXt = (tbEagleEyeXt != null && PRESSURE_TEST_VALUE_1.equals(tbEagleEyeXt.toString()));
            boolean isHsfFlag = (t != null
                && (PRESSURE_TEST_VALUE_1.equals(t.toString()) || PRESSURE_TEST_VALUE_2.equals(t.toString())));
            if (isTbEagleEyeXt || isHsfFlag) {
                result = true;
            }
        }
        return result;
    }

    public static void setThreadLocalData(ProcessorContext context) {
//        setFrameworkThreadLocalData(context);
        setRecThreadLocalData(context);
    }

    public static void clearThreadLocalData() {
//        clearFrameworkThreadLocalData();
        clearRecThreadLocalData();
    }

//    public static void setFrameworkThreadLocalData(ProcessorContext context) {
//        if (Objects.nonNull(context) && Objects.nonNull(context.getTppThreadLocalData())) {
//            ThreadLocalParams.restore(context.getTppThreadLocalData());
//        }
//        if (Objects.nonNull(context) && Objects.nonNull(context.getEagleEyeContext())) {
//            EagleEye.setRpcContext(context.getEagleEyeContext());
//        }
//        if (Debugger.isEnableProfiler() || Debugger.isLocal()) {
//            if (Objects.nonNull(context) && Objects.nonNull(context.getProfilerStartEntry())) {
//                // set current thread profiler entry to tpp solution profiler entry
//                setEntryStack(context.getProfilerStartEntry());
//            }
//        }
//    }
//
//    public static void clearFrameworkThreadLocalData() {
//        ThreadLocalParams.clear();
//        EagleEye.clearRpcContext();
//        if (Debugger.isEnableProfiler() || Debugger.isLocal()) {
//            // reset current profiler entry
//            Profiler.reset();
//        }
//    }

    public static void setRecThreadLocalData(ProcessorContext context) {
        if (Objects.nonNull(context)) {
            CONTEXT.set(context.getTppContext());
            ENABLE_TPP_LAYER.set(context.isEnableTppLayer());
            ENABLE_MANUAL_LAYER.set(context.isEnableManualLayer());
//            MANUAL_HYPERSPACE_RESULT.set(context.getManualHyperspaceResult());
            MANUAL_HYPERSPACE_PARAMS_RESULT.set(context.getManualParamsResult());
            TPP_HYPERSPACE_PARAMS_RESULT.set(context.getTppParamsResult());
            TPP_HYPERSPACE_PARAMS_JSON_RESULT.set(context.getTppParamsJsonResult());
            ENABLE_STREAM_LAZY.set(context.isStreamLazy());
            WRITE_I_GRAPH_RT_TO_TT.set(context.isWriteIGraphRtToTT());
            WRITE_BE_RT_TO_TT.set(context.isWriteBERtToTT());
            ASYNC_IGRAPH.set(context.isAsyncIGraph());
            ASYNC_IGRAPH_TIME_OUT.set(context.getAsyncIGraphTimeOut());
            MULTI_THREAD_EXECUTE.set(context.isMultiThreadExecute());
            USE_TPP_THREAD_POOL.set(context.isUseTppThreadPool());
            USE_NEW_WISP_THREAD_POOL.set(context.isUseNewWispThreadPool());
            EXECUTE_SOLUTION.set(context.getExecuteSolution());
            OFF_RANDOM.set(context.isOffRandom());
            MOCK_TPP_CONFIG.set(context.getMockTppConfig());
            TPP_DEBUG_SAMPLE.set(context.isSampleDebug());
            //            RICH_DEBUG.set(context.isRichDebug());
        }
    }

    public static void clearRecThreadLocalData() {
        FIELDS.values().forEach(ThreadLocal::remove);
    }

    public static void initLoggerAndDebugger(ProcessorContext processorContext) {
        if (Objects.nonNull(processorContext)) {
            Logger.init(processorContext);
            Debugger.init(processorContext);
        }
    }

    public static void clearLoggerAndDebugger() {
        Logger.clear();
        Debugger.clear();
    }

    public static void initAllThreadLocal(ProcessorContext processorContext) {
        initLoggerAndDebugger(processorContext);
        setThreadLocalData(processorContext);
    }

    public static void clearAllThreadLocal() {
        clearThreadLocalData();
        clearLoggerAndDebugger();
    }

    public static Context getContext() {
        return CONTEXT.get();
    }

    public static Boolean isEnableTppLayer() {
        return ENABLE_TPP_LAYER.get();
    }

    public static Boolean isEnableManualLayer() {
        return ENABLE_MANUAL_LAYER.get();
    }

//    public static TppHyperspaceResult getManualHyperspaceResult() {
//        return MANUAL_HYPERSPACE_RESULT.get();
//    }

    public static Map<String, JSONObject> getManualHyperspaceParamsResult() {
        return MANUAL_HYPERSPACE_PARAMS_RESULT.get();
    }

    public static Map<String, String> getTppHyperspaceParamsResult() {
        return TPP_HYPERSPACE_PARAMS_RESULT.get();
    }

    public static Map<String, JSONObject> getTppHyperspaceParamsJsonResult() {
        return TPP_HYPERSPACE_PARAMS_JSON_RESULT.get();
    }

    public static Boolean isEnableStreamLazy() {
        return ENABLE_STREAM_LAZY.get();
    }

    public static Boolean isWriteIGraphRtToTt() {
        return WRITE_I_GRAPH_RT_TO_TT.get();
    }

    public static Boolean isWriteBeRtToTt() {
        return WRITE_BE_RT_TO_TT.get();
    }

    public static Boolean isAsyncIgraph() {
        return ASYNC_IGRAPH.get();
    }

    public static Integer getAsyncIgraphTimeOut() {
        return ASYNC_IGRAPH_TIME_OUT.get();
    }

    public static Boolean isMultiThreadExecute() {
        return MULTI_THREAD_EXECUTE.get();
    }

    public static Boolean isUseTppThreadPool() {
        return USE_TPP_THREAD_POOL.get();
    }

    public static Boolean isUseNewWispThreadPool() {
        return USE_NEW_WISP_THREAD_POOL.get();
    }

    public static String getExecuteSolution() {
        return EXECUTE_SOLUTION.get();
    }

    public static Boolean isOffRandom() {
        return OFF_RANDOM.get();
    }

    public static JSONObject getMockTppConfig() {
        return MOCK_TPP_CONFIG.get();
    }
    
    public static void setMockTppConfig(JSONObject value) {
    	MOCK_TPP_CONFIG.set(value);
    }

}
