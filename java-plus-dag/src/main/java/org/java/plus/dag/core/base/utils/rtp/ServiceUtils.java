//package org.java.plus.dag.core.base.utils.rtp;
//
//import java.nio.charset.Charset;
//import java.util.List;
//import java.util.concurrent.Future;
//
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//
//import com.google.gson.Gson;
//import com.taobao.recommendplatform.protocol.concurrent.AsyncResult;
//import com.taobao.recommendplatform.protocol.domain.rtpClient.RtpConfiguration;
//import com.taobao.recommendplatform.protocol.service.OfficialRTPService;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
//import org.java.plus.dag.core.base.constants.ConstantsFrame;
//import org.java.plus.dag.core.base.constants.TppCounterNames;
//import org.java.plus.dag.core.base.exception.StatusType;
//import org.java.plus.dag.core.base.model.ProcessorContext;
//import org.java.plus.dag.core.base.utils.CommonMethods;
//import org.java.plus.dag.core.base.utils.Debugger;
//import org.java.plus.dag.core.base.utils.Logger;
//import org.java.plus.dag.core.base.utils.SolutionConfig;
//import org.java.plus.dag.core.base.utils.SolutionConfigUtil;
//import com.taobao.rtp_client.RtpRequest;
//import com.taobao.rtp_client.RtpResult;
//import com.taobao.rtp_client.RtpResult.DocUnit;
//import com.taobao.rtp_client.ServiceManager;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.commons.lang3.exception.ExceptionUtils;
//import org.apache.http.HttpEntity;
//import org.apache.http.HttpResponse;
//import org.apache.http.HttpStatus;
//import org.apache.http.StatusLine;
//import org.apache.http.client.HttpResponseException;
//import org.apache.http.util.EntityUtils;
//
///**
// * @author seven.wxy
// * @date 2018/10/22
// */
//public class ServiceUtils {
//    /**
//     * Async Call Rtp Service
//     * @param processorContext
//     * @param request
//     * @param rtpConfiguration
//     * @param printErrorRequest
//     * @param graphRequest
//     * @return graphRequest=true return Future<ServiceManager>, graphRequest=false return Future<RtpMessage>
//     */
//    public static Future<Object> asyncRtpService(ProcessorContext processorContext, RtpRequest request,
//                                                 RtpConfiguration rtpConfiguration, boolean printErrorRequest,
//                                                 boolean graphRequest) {
//        OfficialRTPService rtpService = ServiceFactory.getOfficialRTPService();
//        Future result = null;
//        try {
//            if (graphRequest) {
//                result = rtpService.asyncScoreWithPb2Object(request, rtpConfiguration);
//            } else {
//                result = rtpService.asyncScoreWithObject(request, rtpConfiguration);
//            }
//            debugRtp(rtpService, request, rtpConfiguration, graphRequest);
//        } catch (Exception e) {
//            if (printErrorRequest) {
//                Logger.onlineWarn(String
//                        .format("Domain %s biz %s request is %s ,error: %s", rtpConfiguration.getDomain(), request.getBiz(),
//                                request, ExceptionUtils.getStackTrace(e)));
//            } else {
//                Logger.onlineWarn(String
//                        .format("Domain %s biz %s,error: %s", rtpConfiguration.getDomain(), request.getBiz(), e.getMessage()));
//            }
//            Debugger.exception(ServiceUtils.class, StatusType.INVOKE_RTP_EXCEPTION, e);
//            ServiceFactory.getTPPCounter()
//                          .countSum(TppCounterNames.RTP_EXCEPTION.getCounterName() + "-request-" + request.getBiz(), 1);
//        }
//        return result;
//    }
//
//    /**
//     * Call Rtp Service
//     * @param processorContext
//     * @param request
//     * @param rtpConfiguration
//     * @param printErrorRequest
//     * @param graphRequest
//     * @return graphRequest=true return ServiceManager, graphRequest=false return RtpMessage
//     */
//    public static Object rtpService(ProcessorContext processorContext, RtpRequest request,
//                                    RtpConfiguration rtpConfiguration, boolean printErrorRequest, boolean graphRequest) {
//        long start = System.currentTimeMillis();
//        OfficialRTPService rtpService = ServiceFactory.getOfficialRTPService();
//        Object msg = null;
//        try {
//            if (graphRequest) {
//                msg = rtpService.scoreWithPb2ObjectResult(request, rtpConfiguration);
//            } else {
//                msg = rtpService.scoreWithObjectResult(request, rtpConfiguration);
//            }
//            debugRtp(rtpService, request, rtpConfiguration, graphRequest);
//        } catch (Exception e) {
//            if (printErrorRequest) {
//                Logger.onlineWarn(String
//                        .format("Domain %s biz %s request is %s ,error: %s", rtpConfiguration.getDomain(), request.getBiz(),
//                                request, ExceptionUtils.getStackTrace(e)));
//            } else {
//                Logger.onlineWarn(String
//                        .format("Domain %s biz %s,error: %s", rtpConfiguration.getDomain(), request.getBiz(), e.getMessage()));
//            }
//            Debugger.exception(ServiceUtils.class, StatusType.INVOKE_RTP_EXCEPTION, e);
//            ServiceFactory.getTPPCounter()
//                          .countSum(TppCounterNames.RTP_EXCEPTION.getCounterName() + "-request-" + request.getBiz(), 1);
//        }
//        long cost = System.currentTimeMillis() - start;
//        writeRtpCost(processorContext, request, cost);
//        return msg;
//    }
//
//    public static JSONObject getRtpJson(List<DocUnit> units) {
//        JSONArray arrayList = new JSONArray();
//        for (RtpResult.DocUnit docUnit : units) {
//            JSONArray unitListStr = new JSONArray();
//            long docId = docUnit.getId();
//            unitListStr.add("doc_id:" + docId);
//            unitListStr.add("score:" + docUnit.getScore());
//            unitListStr.add("debug_info:" + docUnit.getDebugInfo());
//            arrayList.add(unitListStr);
//        }
//        JSONObject resultJson = new JSONObject();
//        resultJson.put("docs", arrayList);
//        return resultJson;
//    }
//
//    private static void debugRtp(OfficialRTPService rtpService, RtpRequest request, RtpConfiguration rtpConfiguration, boolean graphRequest) {
//        try {
//            if (Debugger.isRtpDebug()) {
//                request.setDebug(true);
//                if (graphRequest) {
//                    ServiceManager message = rtpService.getResult(rtpConfiguration.getDomain(), request, 20000);
//                    Debugger.put(ServiceUtils.class, () -> "rtpDebug", () -> message == null ? "NULL" : new Gson().toJson(message.getRawGraphResponse()));
//                } else {
//                    RtpResult.RtpMessage debugResult = rtpService.scoreWithObjectResult(rtpConfiguration.getDomain(), request, 20000);
//                    String msgDebug = getRtpJson(debugResult.getUnitsList()).toJSONString();
//                    msgDebug = StringUtils.toEncodedString(msgDebug.getBytes(Charset.forName("GBK")),
//                            Charset.forName("GB2312"));
//                    Debugger.put(ServiceUtils.class, "rtpDebug", msgDebug);
//                }
//                request.setDebug(false);
//            }
//        } catch (Exception e) {
//            Logger.onlineWarn(String.format("Domain %s biz %s request is %s ,error: %s", rtpConfiguration.getDomain(), request.getBiz(), request, ExceptionUtils
//                    .getStackTrace(e)));
//        }
//    }
//
//    public static void writeRtpCost(ProcessorContext processorContext, RtpRequest request, long cost) {
//        if (processorContext.isWriteRTPRtToTT()) {
//            String content = CommonMethods.getRtTTRecord(ConstantsFrame.TYPE_RTP, request.getBiz(), cost);
//            CommonMethods.writeWithDiscard(ConstantsFrame.RT_TT_TOPIC, ConstantsFrame.RT_TT_ACCESS_KEY, content);
//        }
//    }
//
//    @SuppressWarnings("Duplicates")
//    public static String httpAsyncGet(String request, boolean vipServerDomain, int timeout) {
//        String ret = StringUtils.EMPTY;
//        boolean async = SolutionConfigUtil.getSolutionConfig(SolutionConfig.SERVICE_ASYNC_HTTP);
//        try {
//            if (!async) {
//                ret = ServiceFactory.getHttpClient().doGet(request, response -> {
//                            StatusLine statusLine = response.getStatusLine();
//                            HttpEntity entity = response.getEntity();
//                            if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
//                                EntityUtils.consume(entity);
//                                throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
//                            }
//                            return entity == null ? "" : EntityUtils.toString(entity, "UTF-8");
//                        },
//                        true);
//            } else {
//                AsyncResult<HttpResponse> result = ServiceFactory.getHttpClientExtend()
//                                                                 .asyncGet(request, vipServerDomain, timeout);
//                if (result != null) {
//                    HttpResponse response = result.getResult(timeout);
//                    if (response != null) {
//                        if (response.getStatusLine() == null || response.getStatusLine().getStatusCode() != 200) {
//                            EntityUtils.consume(response.getEntity());
//                            throw new HttpResponseException(response.getStatusLine().getStatusCode(),
//                                    response.getStatusLine().getReasonPhrase());
//                        }
//                        HttpEntity entity = response.getEntity();
//                        ret = entity == null ? StringUtils.EMPTY : EntityUtils.toString(entity, "UTF-8");
//                    }
//                }
//            }
//        } catch (Exception e) {
//            ServiceFactory.getTPPCounter().countSum(TppCounterNames.HTTP_CLIENT_EXTEND_ERROR.getCounterName(), 1);
//            Logger.onlineWarn("Request=" + request + ",error:" + ExceptionUtils.getStackTrace(e));
//        }
//        return ret;
//    }
//
//}
