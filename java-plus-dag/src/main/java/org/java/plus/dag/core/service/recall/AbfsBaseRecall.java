package org.java.plus.dag.core.service.recall;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.collect.Lists;
//import com.taobao.recommendplatform.protocol.domain.abfs.AbfsNewResult;
//import com.taobao.recommendplatform.protocol.domain.abfs.AbfsPersonalizerResult;
//import com.taobao.recommendplatform.protocol.domain.abfs.AbfsRequest;
//import com.taobao.recommendplatform.protocol.domain.abfs.AbfsRequestBuilder;
//import com.taobao.recommendplatform.protocol.service.AbfsNewService;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
import org.java.plus.dag.core.base.annotation.ConfigInit;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.constants.TppCounterNames;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.CommonMethods;
import org.java.plus.dag.core.base.utils.Debugger;
import org.java.plus.dag.core.base.utils.Logger;
import org.apache.commons.lang3.StringUtils;

/**
 * @author seven.wxy
 * @date 2019/7/8
 */
public class AbfsBaseRecall extends BaseRecall {
    private static final String ABFS_PATH = "abfs";
    private static final String ABFS_PERSONALIZER = "abfs_personalizer";

    @ConfigInit(desc = "abfs's vipserver address")
    protected String vipServer = "com.taobao.search.abfs_new.vipserver";

    @ConfigInit(desc = "the key of abfs's feature")
    protected String table = StringUtils.EMPTY;

    @ConfigInit(desc = "timeout of abfs reqest")
    protected Integer timeout = 50;

    @ConfigInit(desc = "the format of abfs's return")
    protected String format = "fb2";

    @ConfigInit(desc = "abfs path")
    protected String abfsPath = ABFS_PATH;

    @Override
    public DataSet<Row> doProcess(ProcessorContext processorContext, DataSet<Row> dataSet) {
        //step1:build abfs request
//        AbfsRequest abfsRequest = buildRequest(processorContext);
        //step2:get abfs result
        DataSet<Row> result =null;// getAbfsResult(processorContext, abfsRequest);
        return result;
    }

    /**
     * step1:build abfs request
     *
     * @param context
     * @return AbfsRequest
     */
//    private AbfsRequest buildRequest(ProcessorContext context) {
//        AbfsRequestBuilder requestBuilder = new AbfsRequestBuilder()
//            .setPath(abfsPath)
//            .addFeatures(table)
//            .setFormat(format);
//        requestBuilder = addExtParamsToRequest(context, requestBuilder);
//        AbfsRequest abfsRequest = requestBuilder.build();
//        abfsRequest.setVipserverDomain(vipServer);
//        abfsRequest.setTimeout(Debugger.isLocal() ? ConstantsFrame.PROCESSOR_DEBUG_TIMEOUT_MS : timeout);
//        return abfsRequest;
//    }
//
//    protected AbfsRequestBuilder addExtParamsToRequest(ProcessorContext context, AbfsRequestBuilder requestBuilder) {
//        return requestBuilder;
//    }

    /**
     * step2:get abfs result
     *
     * @param abfsRequest
     * @return AbfsResult
     */
//    protected DataSet<Row> getAbfsResult(ProcessorContext processorContext, AbfsRequest abfsRequest) {
//        long start = System.currentTimeMillis();
//        DataSet<Row> result = new DataSet<>();
//        AbfsNewService abfsService = ServiceFactory.getAbfsNewService();
//        try {
//            if (Debugger.isLocal()) {
//                Logger.info(() -> "Request ABFS:\n" + abfsRequest.toQueryString());
//            }
//            if (async && !Debugger.isLocal()) {
//                Future<AbfsNewResult> asyncResult = abfsService.asyncQuery(abfsRequest);
//                result.setAsyncData(asyncResult, resultParseFunction(processorContext));
//            } else {
//                AbfsNewResult abfsResult = abfsService.query(abfsRequest);
//                if (abfsResult != null) {
//                    AbfsPersonalizerResult abfsPersonalizerResult = abfsResult.getPersonalizerResults().get(ABFS_PERSONALIZER);
//                    result.setData(parseAbfsResult(abfsPersonalizerResult));
//                }
//            }
//            long diff = System.currentTimeMillis() - start;
//            if (!async) {
//                writeRtToTT(processorContext, diff, table);
//            }
//            Debugger.put(this, "AbfsService_cost", diff);
//            ServiceFactory.getTPPCounter().countSum(TppCounterNames.ABFS_RT.getCounterName(), diff);
//        } catch (Exception e) {
//            ServiceFactory.getTPPCounter().countSum(TppCounterNames.ABFS_REQUEST_EXCEPTION.getCounterName(), 1);
//            Logger.onlineWarn("AbfsRecall query failed,msg:" + e.getMessage());
//        }
//        return result;
//    }

//    protected Function<Object, List<Row>> resultParseFunction(ProcessorContext processorContext) {
//        final Function<Object, List<Row>> transformOnCompleteFunction = (response) -> {
//            List<Row> rowList = Lists.newArrayList();
//            long start = System.currentTimeMillis();
//            try {
//                Future<AbfsNewResult> asyncFuture = (Future<AbfsNewResult>)response;
//                if (Objects.nonNull(asyncFuture)) {
//                    AbfsNewResult abfsResult = asyncFuture.get(timeout, TimeUnit.MILLISECONDS);
//                    if (Objects.nonNull(abfsResult)) {
//                        AbfsPersonalizerResult abfsPersonalizerResult = abfsResult.getPersonalizerResults().get(ABFS_PERSONALIZER);
//                        rowList = parseAbfsResult(abfsPersonalizerResult);
//                    }
//                }
//            } catch (Exception e) {
//                rowList = Lists.newArrayList();
//                ServiceFactory.getTPPCounter().countSum(TppCounterNames.ABFS_PARSE_EXCEPTION.getCounterName(), 1);
//                Logger.error("transform abfs async result failure", e);
//            }
//            if (async) {
//                writeRtToTT(processorContext, (System.currentTimeMillis() - start), table);
//            }
//            return rowList;
//        };
//        return transformOnCompleteFunction;
//    }

    /**
     * step3:parse abfs result
     *
     * @param abfsResult
     * @return DataSet
     */
//    protected List<Row> parseAbfsResult(AbfsPersonalizerResult abfsResult) {
//        List<Row> rowList = Lists.newArrayList();
//        if (abfsResult == null) {
//            return rowList;
//        }
//        Row row = new Row();
//        row.setFieldValue(AllFieldName.features, abfsResult);
//        rowList.add(row);
//        return rowList;
//    }

    public static void writeRtToTT(ProcessorContext processorContext, long cost, String table) {
        if (processorContext.isWriteAbfsRtToTT()) {
            String content = CommonMethods.getRtTTRecord(ConstantsFrame.TYPE_ABFS, table, cost);
            CommonMethods.writeWithDiscard(ConstantsFrame.RT_TT_TOPIC, ConstantsFrame.RT_TT_ACCESS_KEY, content);
        }
    }
}
