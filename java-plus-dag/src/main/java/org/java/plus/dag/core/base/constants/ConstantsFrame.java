package org.java.plus.dag.core.base.constants;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.java.plus.dag.core.base.model.ProcessorConfig;

/**
 * Only frame constants in this class
 */
public class ConstantsFrame {
    public static final String TYPE_REQUEST = "request";
    public static final String TYPE_PROCESSOR = "processor";
    public static final String TYPE_IGRAPH = "igraph";
    public static final String TYPE_BE = "be";
    public static final String TYPE_EE = "ee";
    public static final String TYPE_RTP = "rtp";
    public static final String TYPE_ABFS = "abfs";
    public static final String TYPE_TAIR = "tair";
    public static final String TYPE_FUNCTION = "function";

    public static final String LDB_LOCAL_CONTEXT_DATA_VERSION = "LDB_LOCAL_CONTEXT_DATA_VERSION";
    public static final String TT_CONTEXT_CONNECT_STR = "\007";

    public static final String RT_TT_TOPIC = "real_tt_topic_name";
    public static final String RT_TT_ACCESS_KEY = "real_tt_topic_name_access_key";

    public static final String ONLINE_DEBUG_TT_TOPIC = "real_tt_topic_name";
    public static final String ONLINE_DEBUG_TT_ACCESS_KEY = "real_tt_topic_name_access_key";

    public static final String OFFLINE_DEBUG_TT_TOPIC = "real_tt_topic_name";
    public static final String OFFLINE_DEBUG_TT_ACCESS_KEY = "real_tt_topic_name_access_key";

    public static final String PACKAGE_NAME = "org.java.plus.dag.";

    public static final String CONTENT_ID = "content_id";
    public static final String CHANNEL_SESSION_PREFIX = "tianbian_channel_key";

    public static final String JSON = "json";
    public static final String PB = "pb";

    public static final String INT_TYPE = "int";
    public static final String DOUBLE_TYPE = "double";

    public static final String DEFAULT_CONFIG_VALUE = "__DEFAULT__CONFIG__VALUE__";
    public static final String MAIN_CHAIN_KEY = "__MAIN_CHAIN_DATA__";

    public static final String ASYNC = "async";
    public static final String PROCESSOR_TIMEOUT = "processorTimeout";
    public static final String MANUAL_INSTANCE_KEY = "manualInstanceKey";
    public static final String MANUAL_CONFIG_TAIR_PREFIX = "ykrec_meddle_config_";

    public static final Integer PROCESSOR_DEFAULT_TIMEOUT_MS = 100;
    public static final Integer PROCESSOR_DEBUG_TIMEOUT_MS = 10000;

    public static final String INSERT_CONTEXT_KEY_ALG_INFO = "ALGINFO_";

    public static final JSONObject NULL_JSON_OBJECT = new JSONObject(0);
    public static final JSONArray NULL_JSON_ARRAY = new JSONArray(0);
    public static final ProcessorConfig EMPTY_CONFIG = new ProcessorConfig();

    public static final String SCG_CACHE_PREFIX = "content2scg_";
    public static final String CONTENT_ID_PREFIX = "content_id_";
    public static final String MEDDLE_TAIR_PREFIX = "yt_ufo_";

    /**
     * extra dispatch data map key in contextData
     */
    public static final String CONTEXT_EXTRA_DISPATCH_DATA_MAP = "EXTRA_DISPATCH_DATA_MAP";

    /**
     * extra dispatch config mapping key in contextData
     */
    public static final String CONTEXT_EXTRA_DISPATCH_CONFIG_MAPPING = "EXTRA_DISPATCH_CONFIG_MAPPING";

    /**
     * extra dispatch pool type mapping key in contextData
     */
    public static final String CONTEXT_EXTRA_DISPATCH_POOL_TYPE_MAPPING = "EXTRA_DISPATCH_POOL_TYPE_MAPPING";

    /**
     * extra pos extData map in contextData
     */
    public static final String CONTEXT_EXTRA_POS_EXT_DATA = "EXTRA_POS_EXT_DATA";

    /**
     * dynamic ratio key in contextData
     */
    public static final String CONTEXT_DYNAMIC_RATIO_CONFIG = "DYNAMIC_RATIO_CONFIG";

    /**
     * dynamic item type key in contextData
     */
    public static final String CONTEXT_DYNAMIC_ITEM_TYPE = "DYNAMIC_ITEM_TYPE";


    public static final String IGRAPH_DATA_SOURCE_NAME = "core/ds/IGraphDataSourceBase";
}
