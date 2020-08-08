package org.java.plus.dag.core.ds.parser;

import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.DbsRequestBuilder;
import org.java.plus.dag.core.base.utils.JsonUtils;
import org.java.plus.dag.core.base.utils.VariableReplaceUtil;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfig;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfigPojo;
import lombok.Getter;
import lombok.Setter;

/**
 * @author seven.wxy
 * @date 2019/11/12
 */
public class DbsRequestHandler implements IGraphRequestHandler {
    @Getter
    @Setter
    private IGraphDataSourceConfig iGraphDataSourceConfig;

    public DbsRequestHandler(IGraphDataSourceConfig iGraphDataSourceConfig) {
        this.iGraphDataSourceConfig = iGraphDataSourceConfig;
    }

    @Override
    public DataSet<Row> retrieveProcess(ProcessorContext context, IGraphTypeHandler typeHandler) {
        IGraphDataSourceConfigPojo config = iGraphDataSourceConfig.getSingleConfig();
        JSONObject dbsConfig = config.getDbsConfig();
        String vipServer = JsonUtils.getParam(dbsConfig, "vipServer", DbsRequestBuilder.DEFAULT_VIP_SERVER);
        String outFmt = JsonUtils.getParam(dbsConfig, "outFmt", DbsRequestBuilder.DEFAULT_OUTFMT);
        String s = JsonUtils.getParam(dbsConfig, "s", DbsRequestBuilder.DEFAULT_TURING_SERVICE);
        String reqPrefix = JsonUtils.getParam(dbsConfig, "reqPrefix", DbsRequestBuilder.DEFAULT_REQ_PREFIX);
        Integer timeout = JsonUtils.getParam(dbsConfig, "timeout", DbsRequestBuilder.DEFAULT_TIMEOUT_MS);
        String viewName = JsonUtils.getParam(dbsConfig, "viewName", DbsRequestBuilder.DEFAULT_VIEW_NAME);
        JSONObject extraParams = JsonUtils.getParam(dbsConfig, "extraParams", null);
        // only support one view result, DataSet parameter replace use context param
        Map<String, List<JSONObject>> result = DbsRequestBuilder.prepareRequest()
            .vipServer(vipServer).outFmt(outFmt).turingService(s).reqPrefix(reqPrefix)
            .pvId(context.getRequestId()).timeoutMs(timeout).viewName(viewName)
            .addParam(VariableReplaceUtil.parseExtraParams(context, new DataSet<>(), extraParams))
            .sendRequest();
        return DbsRequestBuilder.parseResult(viewName, result.get(viewName));
    }

}
