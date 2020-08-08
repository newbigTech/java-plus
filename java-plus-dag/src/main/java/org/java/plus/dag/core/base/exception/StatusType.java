package org.java.plus.dag.core.base.exception;

import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Map;

public enum StatusType {
    SUCCESS(0, "ok"),
    IGRAPH_EXCEPTION(1, "igraph exception"),
    UNKNOWN_REASON(2, "unkown reason"),
    GENE_SOLUTION_FAILED(3, "proxy solution gene real failed "),
    UTDID_EMPTY(4, "utdid is empty"),
    PARAM_PARSE_EXCEPTION(5, "param parse exception"),
    INVOKE_DONGFENG_EXCEPTION(6, "invoke dongfeng exception"),
    INVOKE_RTP_EXCEPTION(7, "invoke rtp exception"),
    INVOKE_HTTP_EXCEPTION(8, "invoke http exception"),
    INVOKE_TRANSFER_EXCEPTION(9, "transfer exception"),
    DOPROCESS_EXCEPTION(10, "doProcess exception"),
    SOLUTION_INVOKER_EXCEPTION(11, "SolutionInvoker exception"),
    PN_INVALID(12, "pn is not valid"),
    DOMATCH_EXCEPTION(13, "doMatch exception"),
    BE_SERVICE_EXCEPTION(14, "be service exception"),
    CONFIG_INIT_EXCEPTION(15, "config init exception"),;
    private int status;
    private final String msg;
    public static Map<Integer, String> codeMsgMap = Maps.newHashMap();

    static {
        Arrays.stream(
            StatusType.values()).forEach(p -> {
            codeMsgMap.put(p.status, p.toString());
        });
    }

    StatusType(int status, String msg) {
        this.status = status;
        this.msg = msg;
    }

    public int getStatus() {
        return status;
    }

    public String getMsg() {
        return msg;
    }

}
