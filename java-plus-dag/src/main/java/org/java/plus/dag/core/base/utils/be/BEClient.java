//package org.java.plus.dag.core.base.utils.be;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import java.util.function.Function;
//
//import com.google.common.collect.Lists;
//import com.taobao.recommendplatform.protocol.concurrent.AsyncResult;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
//import com.taobao.recommendplatform.protocol.service.dii.DIIRequest;
//import com.taobao.recommendplatform.protocol.service.dii.DIIResponse;
//import org.java.plus.dag.core.base.constants.ConstantsFrame;
//import org.java.plus.dag.core.base.constants.StringPool;
//import org.java.plus.dag.core.base.constants.TppCounterNames;
//import org.java.plus.dag.core.base.model.DataSet;
//import org.java.plus.dag.core.base.model.ProcessorContext;
//import org.java.plus.dag.core.base.model.Row;
//import org.java.plus.dag.core.base.utils.CommonMethods;
//import org.java.plus.dag.core.base.utils.Debugger;
//import org.java.plus.dag.core.base.utils.Logger;
//import org.java.plus.dag.core.base.utils.RandomUtils;
//import org.java.plus.dag.core.base.utils.ThreadLocalUtils;
//import org.java.plus.dag.core.ds.model.BEDataSourceConfig;
//import org.java.plus.dag.core.ds.model.BeAsyncObject;
//import org.apache.commons.collections4.MapUtils;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.commons.lang3.exception.ExceptionUtils;
//
///**
// * @author seth.zjw
// * @version V1.0
// * @Title: BEClient
// * @Package org.java.plus.dag.utils
// * @date 2018/10/10 10:51
// */
//public class BEClient {
//    private int returnCount = 5000;
//    private String vipServer;
//    private String bizName;
//    private String searcher;
//    private String proxyer;
//    private String outfmt = "json";
//    private int timeout;
//    private Map<String, Object> requestExtendParam = new HashMap<>();
//    private BEDataSourceConfig beDataSourceConfig;
//    private String parentInstanceKey;
//
//    public BEClient(BEDataSourceConfig beDataSourceConfig, String parentInstanceKey) {
//        this.init(beDataSourceConfig);
//        this.parentInstanceKey = parentInstanceKey;
//    }
//
//    public BEClient(BEDataSourceConfig beDataSourceConfig, Map<String, String> paramMap, String parentInstanceKey) {
//        this.init(beDataSourceConfig);
//        this.addParamMap(paramMap);
//        this.parentInstanceKey = parentInstanceKey;
//    }
//
//    public void addParamMap(Map<String, String> paramMap) {
//        if (MapUtils.isNotEmpty(paramMap)) {
//            requestExtendParam.putAll(paramMap);
//        }
//    }
//
//    private void checkConfig(BEDataSourceConfig beDataSourceConfig) {
//        //init param
//        this.beDataSourceConfig = beDataSourceConfig;
//
//        returnCount = beDataSourceConfig.getReturnCount();
//        vipServer = beDataSourceConfig.getVipServer();
//        bizName = beDataSourceConfig.getBizName();
//        searcher = beDataSourceConfig.getSearcher();
//        proxyer = beDataSourceConfig.getProxyer();
//        outfmt = beDataSourceConfig.getOutfmt();
//        timeout = beDataSourceConfig.getTimeout();
//
//        requestExtendParam = beDataSourceConfig.getOptionalParam();
//
//        if (StringUtils.isAnyBlank(vipServer, bizName, searcher, proxyer)) {
//            throw new IllegalArgumentException("required vipServer, bizName, searcher, proxyer");
//        }
//    }
//
//    private void init(BEDataSourceConfig beDataSourceConfig) {
//        try {
//            checkConfig(beDataSourceConfig);
//            //Logger.info(() -> "init be client success! beDataSourceConfig=" + beDataSourceConfig.toString());
//        } catch (IllegalArgumentException e) {
//            ServiceFactory.getTPPCounter().countSum(TppCounterNames.BEAN_INIT_ERROR.getCounterName(), 1);
//            Logger.onlineWarn(() -> "be client init failed, wrong config" + e.getMessage());
//            Logger.error(() -> "be client init wrong, init param wrong" + ExceptionUtils.getStackTrace(e), e);
//        } catch (Exception e) {
//            ServiceFactory.getTPPCounter().countSum(TppCounterNames.BEAN_INIT_ERROR.getCounterName(), 1);
//            Logger.onlineWarn(() -> "be client init failed, wrong config" + e.getMessage());
//            Logger.error(() -> "be client init wrong:" + ExceptionUtils.getStackTrace(e), e);
//        }
//    }
//
//    private DIIRequest initRequest(ProcessorContext processorContext) {
//        DIIRequest request = geneRequest(processorContext);
//        //init request param by config
//        if (MapUtils.isNotEmpty(requestExtendParam)) {
//            requestExtendParam.forEach((k, v) -> request.addParam(k, String.valueOf(v)));
//        }
//        if (RandomUtils.isOffRandom()) {
//            request.addParam("shuffle", "false");
//        }
//        return request;
//    }
//
//    protected DIIRequest geneRequest(ProcessorContext context) {
//        DIIRequest request = new DIIRequest();
//        request.setVipDomain(vipServer);
//        request.buildBizName(bizName);
//        request.buildReturnCount(returnCount);
//        request.buildS(searcher);
//        request.setTimeout(timeout);
//        //default is json
//        if (DIIRequest.FORMAT.FB2.toString().equals(outfmt)) {
//            request.setFormat(DIIRequest.FORMAT.FB2);
//        }
//        request.addParam("p", proxyer);
//        request.addParam("utdid", context.getUtdid());
//        return request;
//    }
//
//    protected BeAsyncObject requestBEServiceAsync(DIIRequest request, String proxyName, String searchName) {
//        AsyncResult<DIIResponse> asyncResult = null;
//        long begin = System.currentTimeMillis();
//        try {
//            asyncResult = ServiceFactory.getDIIClient().asyncSearch(request);
//        } catch (Exception ex) {
//            Logger.warn(() -> new StringBuilder("async be request error ").append(proxyName).append("|").append(searchName).append(",msg:").append(ex.getMessage()).toString());
//            ServiceFactory.getTPPCounter().countSum(new StringBuilder(TppCounterNames.BE_ASYNC_REQUEST_ERROR.getCounterName()).append(proxyName).append("__").append(searchName).toString(), 1);
//        }
//
//        if (asyncResult == null) {
//            return new BeAsyncObject(null, null);
//        }
//
//        final Function<Object, List<Row>> transformOnCompleteFunction = (response) -> {
//            List<Row> rowList = Lists.newArrayList();
//            try {
//                AsyncResult<DIIResponse> res = (AsyncResult<DIIResponse>)response;
//                long getBegin = System.currentTimeMillis();
//                if (res != null) {
//                    DIIResponse diiResponse = res.getResult(request.getTimeout());
//                    long parseBegin = System.currentTimeMillis();
//                    Debugger.put(this, () -> proxyName + "__" + searchName + "_be_request_cost", () -> (parseBegin - getBegin));
//                    if (ThreadLocalUtils.isWriteBeRtToTt()) {
//                        String content = CommonMethods.getRtTTRecord(
//                            ConstantsFrame.TYPE_BE, proxyName + "|" + searchName + "|request", parseBegin - begin);
//                        CommonMethods.writeWithDiscard(ConstantsFrame.RT_TT_TOPIC, ConstantsFrame.RT_TT_ACCESS_KEY, content);
//                    }
//                    if (diiResponse != null) {
//                        BEParserFactory beParserFactory = new BEParserFactory(beDataSourceConfig);
//                        rowList = beParserFactory.getParser().parseDetailResponse(diiResponse);
//                        Debugger.put(this, () -> proxyName + "__" + searchName + "_be_parse_rsp_cost", () -> (System.currentTimeMillis() - parseBegin));
//                    }
//                }
//            } catch (Exception e) {
//                Logger.warn(() -> new StringBuilder("async be transform error ").append(proxyName).append("|").append(searchName).append(",msg:").append(e.getMessage()).toString());
//                ServiceFactory.getTPPCounter().countSum(new StringBuilder(TppCounterNames.BE_ASYNC_TRANSFORM_ERROR.getCounterName()).append(proxyName).append("__").append(searchName).toString(), 1);
//                ServiceFactory.getTPPCounter().countSum(TppCounterNames.BE_ASYNC_REQUEST_ERROR_ALL.getCounterName(), 1);
//            }
//            return rowList;
//        };
//
//        return new BeAsyncObject(asyncResult, transformOnCompleteFunction);
//    }
//
//    protected List<Row> requestBEService(DIIRequest request, String proxyName, String searchName) {
//        List<Row> rowList = Lists.newArrayList();
//        try {
//            long begin = System.currentTimeMillis();
//            // 2.request BE
//            DIIResponse response = ServiceFactory.getDIIClient().search(request);
//            long cost = System.currentTimeMillis() - begin;
//            Debugger.put(this, "be_service_request_cost", cost);
//            Logger.info(() -> "Request BE:\n" + request);
//            if (ThreadLocalUtils.isWriteBeRtToTt()) {
//                String content = CommonMethods.getRtTTRecord(ConstantsFrame.TYPE_BE, proxyName + "|" + searchName, cost);
//                CommonMethods.writeWithDiscard(ConstantsFrame.RT_TT_TOPIC, ConstantsFrame.RT_TT_ACCESS_KEY, content);
//            }
//
//            // 3.format
//            Objects.requireNonNull(response);
//            BEParserFactory beParserFactory = new BEParserFactory(beDataSourceConfig);
//
//            // 4.return
//            long parseBegin = System.currentTimeMillis();
//            rowList = beParserFactory.getParser().parseDetailResponse(response);
//            long parseCost = System.currentTimeMillis() - parseBegin;
//            //Logger.info(() -> "parseResponse cost=" + parseCost);
//            Debugger.put(this, "be_service_result_parse_cost", parseCost);
//        } catch (Exception ex) {
//            Logger.warn(() -> new StringBuilder("sync be request error ").append(proxyName).append("|").append(searchName).append(",msg:").append(ex.getMessage()).toString());
//            ServiceFactory.getTPPCounter().countSum(new StringBuilder(TppCounterNames.BE_SYNC_REQUEST_ERROR.getCounterName()).append(proxyName).append("__").append(searchName).toString(), 1);
//        }
//        return rowList;
//    }
//
//    public DataSet<Row> retrieveDataSet(ProcessorContext context, boolean async) {
//        DataSet<Row> rowDataSet = new DataSet<>();
//
//        //1.init request
//        DIIRequest request = initRequest(context);
//        if (context.getDebug()) {
//            Debugger.put(this, getParentKey() + "_be_url", request.toString());
//        }
//        if (async && !Debugger.isLocal()) {
//            BeAsyncObject asyncSearchResult = requestBEServiceAsync(request, proxyer, searcher);
//            if (asyncSearchResult != null) {
//                rowDataSet.setAsyncData(asyncSearchResult.getAsyncQuery(), asyncSearchResult.getAsyncFunction());
//            }
//        } else {
//            List<Row> rowList = requestBEService(request, proxyer, searcher);
//            rowDataSet.setData(rowList);
//        }
//        return rowDataSet;
//    }
//
//    private String getParentKey() {
//        String parentKey = StringUtils.EMPTY;
//        if (StringUtils.isNotEmpty(parentInstanceKey)) {
//            if (StringUtils.contains(parentInstanceKey, StringPool.HASH)) {
//                parentKey = StringUtils.substring(parentInstanceKey, StringUtils.indexOf(parentInstanceKey, StringPool.HASH) + 1);
//            } else {
//                parentKey = parentInstanceKey;
//            }
//        }
//        return parentKey;
//    }
//}
