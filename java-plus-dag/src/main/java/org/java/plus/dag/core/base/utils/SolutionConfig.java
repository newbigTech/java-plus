package org.java.plus.dag.core.base.utils;

import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.model.ParameterConfig;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * Get solution common config by key
 * Config key name rule: ${Usage prefix}-xxx
 *
 * @author seven.wxy
 * @date 2018/10/10
 */
public enum SolutionConfig implements ParameterConfig {
    // Monitor related configuration
    MONITOR_WRITE_REQUEST_RT_TO_TT("Monitor_writeRequestRtToTT", false, "write request rt to TT log or not.")
    ,MONITOR_WRITE_PROCESSOR_RT_TO_TT("Monitor_writeProcessorRtToTT", false, "write processor rt to TT log or not.")
    ,MONITOR_WRITE_I_GRAPH_RT_TO_TT("Monitor_writeIGraphRtToTT", false, "write IGraph rt to TT log or not.")
    ,MONITOR_WRITE_BE_RT_TO_TT("Monitor_writeBERtToTT", false, "write BE rt to TT log or not.")
    ,MONITOR_WRITE_EE_RT_TO_TT("Monitor_writeEERtToTT", false, "write EE rt to TT log or not.")
    ,MONITOR_WRITE_RTP_RT_TO_TT("Monitor_writeRTPRtToTT", false, "write RTP rt to TT log or not.")
    ,MONITOR_WRITE_ABFS_RT_TO_TT("Monitor_writeAbfsRtToTT", false, "write ABFS rt to TT log or not.")
    ,MONITOR_WRITE_RESULT_TO_TT("Monitor_writeResultToTT", false, "write Result rt to TT log or not.")
    ,MONITOR_WRITE_RTP_REQUEST_TO_TT("Monitor_writeRTPRequestToTT", false, "write RTP request to TT log or not.")

    // Two party service related configuration
    ,SERVICE_ASYNC_BE("Service_asyncBE", true, "request be async or not.")
    ,SERVICE_ASYNC_IGRAPH("Service_asyncIGraph", true, "request IGraph async or not.")
    ,SERVICE_ASYNC_IGRAPH_TIME_OUT("Service_asyncIGraphTimeOut", 50, "request IGraph async or not.")
    ,SERVICE_ASYNC_HTTP("Service_asyncHttp", true, "request http async or not.")

    // Feed stream related configuration
    ,FEED_CHANNEL_KEY("Feed_channelKey", "findChannel", "feed stream session cache prefix.")
    ,FEED_SESSION_SHOW_MAX_COUNT("Feed_sessionShowMaxCount", 0, "sessionShowMaxCount")
    ,FEED_SESSION_IGRAPH_SAVE("Feed_sessionIGraphSave", false, "sessionIGraphSave")

    // TPP Track log related configuration
    ,TRACK_LOG_NEED_TIME("TrackLog_needTime", true, "Request id track log need timestamp or not.")

    ,SOLUTION_MULTI_THREAD_EXECUTE("Solution_multiThreadExecute", true, "solution use multi thread execute.")
    ,SOLUTION_USE_TPP_THREAD_POOL("Solution_useTppThreadPool", true, "solution use tpp thread pool or not.")
    ,SOLUTION_USE_NEW_WISP_THREAD_POOL("Solution_useNewWispThreadPool", false, "solution use new wisp thread pool or not.")
    ,SOLUTION_EXECUTE_SOLUTION("Solution_executeSolution", SolutionConfig.THREAD, "use CompletableFuture to control timeout.")
    ,SOLUTION_LOG_STACK_LINE("Solution_logStackLine", 3, "log stack line")
    ,SOLUTION_FUTURE_DEP_ONE("Solution_futureDepOne", true, "use Combine to make future.")
    ,SOLUTION_DISABLE_LOG("Solution_disableLog", false, "disable log or not.")
    ,SOLUTION_LOG_LEVEL("Solution_logLevel", StringUtils.EMPTY, "log level.")
    ,SOLUTION_TIMEOUT_COUNTER("Solution_timeoutCounter", true, "enable processor timeout counter or not.")

    // Solution related configuration
    ,SOLUTION_PROCESSOR("Solution_processor", StringUtils.EMPTY, "Solution start.")
    ,SOLUTION_CONFIG("Solution_config", ConstantsFrame.NULL_JSON_OBJECT, "Solution config.")
    ,SOLUTION_DATASOURCE_TAIR("Solution_datasource_tair", ConstantsFrame.NULL_JSON_OBJECT, "Solution datasource tair key")
    ,SOLUTION_DATASOURCE_BE("Solution_datasource_be", ConstantsFrame.NULL_JSON_OBJECT, "Solution datasource be key")
    ,SOLUTION_DATASOURCE_IGRAPH("Solution_datasource_igraph", ConstantsFrame.NULL_JSON_OBJECT, "Solution datasource iGraph key")
    ,SOLUTION_DATASOURCE_RTP("Solution_datasource_rtp", ConstantsFrame.NULL_JSON_OBJECT, "Solution datasource rtp key")
    ,SOLUTION_STREAM_LAZY_PROCESS("Solution_streamLazy", true, "stream lazy process or not.")
    ,SOLUTION_URL_PARAMETER_ALIAS("Solution_urlParameterAlias", ConstantsFrame.NULL_JSON_OBJECT, "Solution url parameter name alias.")
    ,SOLUTION_ENABLE_PROFILER("Solution_enableProfiler", false, "enable processor cost profiler or not")

    ,SOLUTION_ENABLE_TPP_LAYER("Solution_enableTppLayer", false, "enable tpp layer or not")
    ,SOLUTION_TPP_LAYER_REPLACE_VALUE("Solution_tppLayerReplaceValue", true, "tpp layer replace value or not")
    ,SOLUTION_ENABLE_MANUAL_LAYER("Solution_enableManualLayer", false, "enable tpp layer or not")
    ,SOLUTION_ALGINFO_PREFIX("Solution_algInfo_prefix", StringUtils.EMPTY, "distinguish remote algInfo prefix")

    //DEBUG config
    ,DEBUG_SAMPLE("debug_sample", ConstantsFrame.NULL_JSON_OBJECT,"debug sample rate config key")
    ;

    public static final String INVOKER = "invoker";
    public static final String THREAD = "thread";

    @Getter
    private String name;
    @Getter
    private Object defaultValue;
    @Getter
    private String desc;

    SolutionConfig(String name, Object defaultValue, String desc) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.desc = desc;
    }
}
