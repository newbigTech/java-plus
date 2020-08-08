package org.java.plus.dag.core.base.constants;

/**
 * @author seven.wxy
 * @date 2018/11/23
 */
public enum TppCounterNames {
    /**
     * bean factory counter
     */
    BEAN_CREATE_ERROR("bean_create_error"),
    BEAN_INIT_ERROR("bean_init_error"),

    /**
     * rtp counter
     */
    RTP_EXCEPTION("feed_rtp_exception"),

    /**
     * abfs counter
     */
    ABFS_REQUEST_EXCEPTION("abfs_request_exception"),
    ABFS_RT("abfs_rt"),
    ABFS_PARSE_EXCEPTION("abfs_parse_exception"),

    /**
     * tair counter
     */
    TAIR_INIT_EXCEPTION("tair_init_exception"),
    TAIR_GET_EXCEPTION("tair_get_exception"),
    TAIR_GET_FAIL("tair_get_fail"),
    TAIR_PUT_EXCEPTION("tair_put_exception"),
    TAIR_PUT_RT("tair_put_rt"),
    TAIR_GET_RT("tair_get_rt"),
    TAIR_VERSION_ERROR("tair_version_error"),
    TAIR_PUT_OTHER_ERROR("tair_put_other_error"),
    TAIR_PUT_ERROR_CODE("tair_put_error_code_"),
    TAIR_GET_QPS("tair_get_qps"),
    TAIR_PUT_QPS("tair_put_qps"),
    TAIR_INCR_EXCEPTION("tair_incr_exception"),
    TAIR_INCR_RT("tair_incr_rt"),
    TAIR_INCR_QPS("tair_incr_qps"),
    TAIR_RETRY_TIME("tair_retry_time"),
    TAIR_FORCE_OVERWRITE("tair_force_overwrite"),

    /**
     * processor counter prefix
     */
    PROC_EXCEPTION_COUNTER("PROC_EXCEPTION_"),
    PROC_EMPTY_RESULT("PROC_EMPTY_RESULT_"),
    PROC_TIMEOUT("PROC_TIMEOUT_"),
    PROC_DAG_TIMEOUT("PROC_DAG_TIMEOUT_"),
    PROC_OVER_TIME("PROC_OVER_TIME_"),
    PROC_SKIP("PROC_SKIP_"),
    PROC_RT("PROC_RT_"),

    /**
     * tt counter prefix
     */
    TT_WRITE_ERROR("tt_write_error_"),

    /**
     * be counter prefix
     */
    BE_RESULT_EMPTY("BE_RESULT_EMPTY"),

    /**
     * iGraph counter prefix
     */
    IGRAPH_ASYNC_FUTURE_NULL_ERROR("iGraph_async_future_null_error_"),
    IGRAPH_SEARCH_ASYNC_ERROR("iGraph_search_async_error_"),
    IGRAPH_QUERY_EXCEPTION_BATCH("iGraph_query_exception_batch_"),
    IGRAPH_ASYNC_RESULT_TRANSFORM_ERROR("iGraph_async_result_transform_error_"),
    IGRAPH_RESULT_NULL("iGraph_result_null_"),
    IGRAPH_REQUEST_EMPTY("iGraph_request_empty"),
    IGRAPH_UPDATE_ERROR("iGraph_update_error"),

    /**
     * related recommend session counter
     */
    TAIR_WRITE_SUCCESS_CNT("tair_write_succ_cnt"),
    TAIR_WRITE_ERROR_CNT("tair_write_error_cnt"),

    /**
     * cache counter prefix
     */
    LRU_CACHE_SIZE("LRU_CACHE_SIZE_"),
    LRU_CACHE_HIT_RATE("LRU_CACHE_HIT_RATE_"),
    LRU_CACHE_LOAD_EXP_RATE("LRU_CACHE_LOAD_EXP_RATE_"),

    /**
     * be counter prefix
     */
    BE_ASYNC_REQUEST_ERROR("be_async_request_error_"),
    BE_ASYNC_TRANSFORM_ERROR("be_async_transform_error_"),
    BE_SYNC_REQUEST_ERROR("be_sync_request_error_"),
    BE_ASYNC_REQUEST_ERROR_ALL("BE_ASYNC_REQUEST_ERROR"),

    /**
     * http counter
     */
    HTTP_CLIENT_EXTEND_ERROR("HTTP_CLIENT_EXTEND_ERROR"),

    /**
     * merge processor counter prefix
     */
    TPP_BACKUP("TPP_BACKUP"),

    /**
     * ad counter
     */
    UC_AD_INVOKE_FAILURE("UcInvokeFailure"),

    /**
     * manual hit layer counter
     */
    MANUAL_LAYER_HIT("MANUAL_LAYER_HIT")
    ;

    private String counterName;

    TppCounterNames(String counterName) {
        this.counterName = counterName;
    }

    public String getCounterName() {
        return counterName;
    }

    public void setCounterName(String counterName) {
        this.counterName = counterName;
    }
}
