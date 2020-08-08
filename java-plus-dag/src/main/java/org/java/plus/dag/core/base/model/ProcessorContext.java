package org.java.plus.dag.core.base.model;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

//import com.alibaba.common.lang.diagnostic.Profiler;
import com.alibaba.fastjson.JSONObject;
//import com.alibaba.glaucus.common.SceneTimeoutHolder;
//import com.alibaba.glaucus.common.ThreadLocalParams;

import com.google.common.collect.Maps;
//import com.taobao.eagleeye.EagleEye;
//import com.taobao.eagleeye.RpcContext_inner;
//import com.taobao.recommendplatform.protocol.solution.Context;
//import com.taobao.recommendplatform.protocol.solution.TppHyperspaceResult;
import org.java.plus.dag.core.base.utils.ContextUtil;
import org.java.plus.dag.core.base.utils.HistoryDebugger;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.base.utils.SolutionConfig;
import org.java.plus.dag.core.base.utils.SolutionConfigUtil;
import org.java.plus.dag.solution.Context;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.collections4.MapUtils;

/**
 * @author seven.wxy
 * @date 2018/9/25
 */
@Data
@Accessors(chain = true)
public class ProcessorContext implements Cloneable {
	protected static final String REAL_APP_ID = "appid";
	protected static final String TPP_SCENE_ID = "app_id";
	protected static final String CONTENT_ID = "content_id";

	protected static final String DEBUG_PARAM = "debugParam";
	protected static final String ENV = "env";

	public static final String UTDID_NAME = "utdid";
	public static final String IDS = "ids";
	protected static final String UID_NAME = "uid";
	protected static final String USER_NAME = "user_id";
	protected static final String VID_NAME = "vid";
	protected static final String SID_NAME = "sid";

	protected static final String PN_NAME = "pn";
	protected static final String ADS_PAGE_NO = "adsPageNo";
	protected static final String COUNT_NAME = "count";
	protected static final String ITEM_COUNT_NAME = "item_count";
	protected static final String OFF_RANDOM = "offRandom";
	protected static final String RICH_DEBUG = "richDebug";

	/**
	 * EagleEye rpc context
	 */
//    private RpcContext_inner eagleEyeContext;

	/**
	 * Save tpp thread local data ThreadLocalParams
	 */
	private Map<String, Object> tppThreadLocalData = Maps.newConcurrentMap();

	/**
	 * Save profiler entry
	 */
//    private Profiler.Entry profilerStartEntry;
	/**
	 * Enable profiler or not
	 */
	private boolean enableProfiler;

	private boolean streamLazy;
	private boolean disableLog;
	private int logStackLine;
	private boolean enableTimeoutCounter;

	private boolean writeProcessorRtToTT;
	private boolean writeRequestRtToTT;
	private boolean writeIGraphRtToTT;
	private boolean writeBERtToTT;
	private boolean writeEERtToTT;
	private boolean writeRTPRtToTT;
	private boolean writeAbfsRtToTT;
	private boolean writeResultToTT;
	private boolean writeRTPRequestToTT;
	private boolean asyncIGraph;
	private int asyncIGraphTimeOut;

	private boolean multiThreadExecute;
	private boolean useTppThreadPool;
	private boolean useNewWispThreadPool;
	private String executeSolution;

	private String logLevel;

	private boolean offRandom;
	private boolean richDebug;

	/**
	 * Enable tpp layer or not
	 */
	private boolean enableTppLayer;

	/**
	 * tpp layer replace value or not
	 */
	private boolean tppLayerReplaceValue;

	/**
	 * Enable manual layer or not
	 */
	private boolean enableManualLayer;

	/**
	 * Mock tpp config or not
	 */
	private JSONObject mockTppConfig;

	/**
	 * Tpp scene timeout value
	 */
	private long sceneTimeout;

	/**
	 * manualHyperspaceResult
	 */
//    private TppHyperspaceResult manualHyperspaceResult;

	/**
	 * manualHyperspaceParamsResult
	 */
	private Map<String, JSONObject> manualParamsResult;

	/**
	 * tppHyperspaceParamsResult
	 */
	private Map<String, String> tppParamsResult;

	/**
	 * tppHyperspaceParamsJsonResult
	 */
	private Map<String, JSONObject> tppParamsJsonResult;

	/**
	 * tpp context
	 */
	private Context tppContext;

	/**
	 * tpp layer buckets
	 */
	private String tppBuckets;

	/**
	 * tpp scene id
	 */
	private String realAppId;
	/**
	 * app_id
	 */
	private String appId;
	/**
	 * content_id
	 */
	private String contentId;

	private String debugParam;
	/**
	 * if debugParam is not null, debug is true
	 */
	private Boolean debug = false;
	private Map<String, Object> debugInfo = Maps.newLinkedHashMap();
	/**
	 * WARN:current put the same key, the value will be replaced, please use config
	 * unique key to get and put
	 */
	private Map<String, Object> contextData = Maps.newConcurrentMap();

	private Map<String, Object> trackLog = Maps.newConcurrentMap();

	/**
	 * Output this map to final result extData attribute
	 */
	private Map<String, Object> extData = Maps.newConcurrentMap();

	private Long userId;
	private String utdid;
	private Long uid;
	private String vid;
	private String sid;
	private Integer pn;
	private Integer adsPageNo;
	private Integer count = 10;
	private Integer itemCount = 10;

	private String requestId;
	private String env;

	private Long startTime;

	private boolean sampleDebug;

	private String message;

	public void init(Context tppContext) {
		this.setStartTime(System.currentTimeMillis());
//        this.setEagleEyeContext(EagleEye.getRpcContext());
//        this.setTppThreadLocalData(ThreadLocalParams.save());
//        this.setProfilerStartEntry(Profiler.getEntry());
		// Solution config set to context
		this.setEnableProfiler(SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_ENABLE_PROFILER));
		// layer config
		this.setEnableTppLayer(SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_ENABLE_TPP_LAYER));
		this.setTppLayerReplaceValue(
				SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_TPP_LAYER_REPLACE_VALUE));
		this.setEnableManualLayer(SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_ENABLE_MANUAL_LAYER));

		this.setStreamLazy(SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_STREAM_LAZY_PROCESS));
		this.setDisableLog(SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_DISABLE_LOG));
		this.setLogStackLine(SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_LOG_STACK_LINE));

		this.setWriteProcessorRtToTT(
				SolutionConfigUtil.getSolutionConfig(SolutionConfig.MONITOR_WRITE_PROCESSOR_RT_TO_TT));
		this.setWriteRequestRtToTT(SolutionConfigUtil.getSolutionConfig(SolutionConfig.MONITOR_WRITE_REQUEST_RT_TO_TT));
		this.setWriteEERtToTT(SolutionConfigUtil.getSolutionConfig(SolutionConfig.MONITOR_WRITE_EE_RT_TO_TT));
		this.setWriteRTPRtToTT(SolutionConfigUtil.getSolutionConfig(SolutionConfig.MONITOR_WRITE_RTP_RT_TO_TT));
		this.setWriteAbfsRtToTT(SolutionConfigUtil.getSolutionConfig(SolutionConfig.MONITOR_WRITE_ABFS_RT_TO_TT));
		this.setWriteResultToTT(SolutionConfigUtil.getSolutionConfig(SolutionConfig.MONITOR_WRITE_RESULT_TO_TT));
		this.setWriteRTPRequestToTT(
				SolutionConfigUtil.getSolutionConfig(SolutionConfig.MONITOR_WRITE_RTP_REQUEST_TO_TT));
		this.setWriteIGraphRtToTT(SolutionConfigUtil.getSolutionConfig(SolutionConfig.MONITOR_WRITE_I_GRAPH_RT_TO_TT));
		this.setWriteBERtToTT(SolutionConfigUtil.getSolutionConfig(SolutionConfig.MONITOR_WRITE_BE_RT_TO_TT));
		this.setAsyncIGraph(SolutionConfigUtil.getSolutionConfig(SolutionConfig.SERVICE_ASYNC_IGRAPH));
		this.setAsyncIGraphTimeOut(SolutionConfigUtil.getSolutionConfig(SolutionConfig.SERVICE_ASYNC_IGRAPH_TIME_OUT));
		this.setMultiThreadExecute(SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_MULTI_THREAD_EXECUTE));
		this.setUseTppThreadPool(SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_USE_TPP_THREAD_POOL));
		this.setUseNewWispThreadPool(
				SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_USE_NEW_WISP_THREAD_POOL));
		this.setExecuteSolution(SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_EXECUTE_SOLUTION));
		this.setEnableTimeoutCounter(SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_TIMEOUT_COUNTER));

		// URL params set to context
//        this.setLogLevel(Logger.getLogLevel(tppContext));
		this.setOffRandom(ContextUtil.getBooleanOrDefault(tppContext, OFF_RANDOM, false));
		this.setRichDebug(ContextUtil.getBooleanOrDefault(tppContext, RICH_DEBUG, false));
//        this.setSceneTimeout(SceneTimeoutHolder.getTimeout());

		this.setTppContext(tppContext);
		this.urlParameterAlias(tppContext);
		this.setRealAppId(ContextUtil.getString(tppContext, REAL_APP_ID));
		this.setAppId(ContextUtil.getString(tppContext, TPP_SCENE_ID));
		this.setContentId(ContextUtil.getString(tppContext, CONTENT_ID));
		this.setDebugParam(ContextUtil.getString(tppContext, DEBUG_PARAM));
		this.setDebug(Objects.nonNull(getDebugParam()));
		this.setUserId(ContextUtil.getLong(tppContext, USER_NAME));
		this.setUtdid(ContextUtil.getString(tppContext, UTDID_NAME));
		this.setUid(ContextUtil.getLong(tppContext, UID_NAME));
		this.setVid(ContextUtil.getString(tppContext, VID_NAME));
		this.setSid(ContextUtil.getString(tppContext, SID_NAME));
		this.setPn(ContextUtil.getIntOrDefault(tppContext, PN_NAME, 0));
		this.setAdsPageNo(ContextUtil.getIntOrDefault(tppContext, ADS_PAGE_NO, 0));
		this.setCount(ContextUtil.getIntOrDefault(tppContext, COUNT_NAME, 10));
		this.setItemCount(ContextUtil.getIntOrDefault(tppContext, ITEM_COUNT_NAME, 10));
		this.setEnv(ContextUtil.getString(tppContext, ENV));
		this.setSampleDebug(HistoryDebugger.isSampleWriteToTunel(this));
	}

	private void urlParameterAlias(Context tppContext) {
		JSONObject config = SolutionConfigUtil.getSolutionConfig(SolutionConfig.SOLUTION_URL_PARAMETER_ALIAS);
		if (MapUtils.isNotEmpty(config)) {
			for (Map.Entry<String, Object> entry : config.entrySet()) {
				if (Objects.nonNull(entry.getValue()) && tppContext.containsKey(entry.getKey())) {
					tppContext.put(String.valueOf(entry.getValue()), tppContext.get(entry.getKey()));
				}
			}
		}
	}

	public Map<String, Object> getDebugInfo() {
		return this.debugInfo;
	}

	public ProcessorContext addContextData(String key, Object object) {
		contextData.put(key, object);
		return this;
	}

	public <T> T getContextData(String key) {
		return (T) contextData.get(key);
	}

	public <T> T getContextDataOrDefault(String key, T defaultValue) {
		return (T) contextData.getOrDefault(key, defaultValue);
	}

	public <T> T getContextDataOrDefault(String key, Supplier<T> defaultValueSupplier) {
		T v;
		return (((v = (T) contextData.get(key)) != null) || contextData.containsKey(key)) ? v
				: defaultValueSupplier.get();
	}

	@Override
	public ProcessorContext clone() {
		ProcessorContext context = null;
		try {
			context = (ProcessorContext) super.clone();
			context.setContextData(Maps.newConcurrentMap());
			context.getContextData().putAll(this.getContextData());
		} catch (Exception e) {
			// ignore
		}
		return context;
	}

	public static void mergeContextData(ProcessorContext srcContext, ProcessorContext newContext) {
		newContext.getContextData().forEach((key, value) -> {
			// not allow overwrite exists key
			if (Objects.isNull(srcContext.getContextData(key))) {
				srcContext.addContextData(key, value);
			}
		});
	}

	public Context getTppContext() {
		Context context = null;
		return context;
	}

}
