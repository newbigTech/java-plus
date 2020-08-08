//package org.java.plus.dag.core.base.utils.igraph;
//
//import java.util.List;
//import java.util.Objects;
//import java.util.Set;
//import java.util.concurrent.Future;
//
//import com.google.common.collect.Sets;
//import com.taobao.igraph.client.config.PgConfig;
//import com.taobao.igraph.client.model.AtomicQuery;
//import com.taobao.igraph.client.model.JoinQuery;
//import com.taobao.igraph.client.model.PGQuery;
//import com.taobao.igraph.client.model.QueryResult;
//import com.taobao.igraph.client.model.SingleQueryResult;
//import com.taobao.recommendplatform.protocol.service.IGraphService;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
//import org.java.plus.dag.core.base.constants.ConstantsFrame;
//import org.java.plus.dag.core.base.utils.CommonMethods;
//import org.java.plus.dag.core.base.utils.Logger;
//import org.java.plus.dag.core.base.utils.ThreadLocalUtils;
//import org.apache.commons.collections4.CollectionUtils;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.commons.lang3.exception.ExceptionUtils;
//
//public class IGraphUtil {
//    private static IGraphService iGraphService = ServiceFactory.getIGraphService();
//
//    private static IGraphUtil instance = new IGraphUtil();
//
//    private IGraphUtil() {
//    }
//
//    public static IGraphUtil getInstance() {
//        return instance;
//    }
//
//    public static Future<QueryResult> searchAsync(String exceptionCounterKey, PgConfig pgConfig, PGQuery... queries) {
//        long start = System.currentTimeMillis();
//        Future<QueryResult> result = null;
//        try {
//            result = iGraphService.searchAsync(pgConfig, queries);
//        } catch (Exception e) {
//            String tables = getKey(queries);
//            ServiceFactory.getTPPCounter().countSum(exceptionCounterKey + tables, 1);
//            Logger.onlineWarn(exceptionCounterKey + " cost:" + (System.currentTimeMillis() - start)
//                    + ",table:" + tables + ",StackTrace:" + ExceptionUtils.getStackTrace(e));
//        }
//        return result;
//    }
//
//    public static QueryResult search(String exceptionCounterKey, PgConfig pgConfig, PGQuery... queries) {
//        long start = System.currentTimeMillis();
//        QueryResult result = null;
//        try {
//            result = iGraphService.search(pgConfig, queries);
//        } catch (Exception e) {
//            String tables = getKey(queries);
//            ServiceFactory.getTPPCounter().countSum(exceptionCounterKey + tables, 1);
//            Logger.onlineWarn(exceptionCounterKey + " cost:" + (System.currentTimeMillis() - start)
//                    + ",table:" + tables + ",StackTrace:" + ExceptionUtils.getStackTrace(e));
//        }
//        long cost = System.currentTimeMillis() - start;
//        writeRtToTT(cost, queries);
//        return result;
//    }
//
//    public static boolean checkSQRNotEmpty(SingleQueryResult singleQueryResult) {
//        return Objects.nonNull(singleQueryResult) && !singleQueryResult.hasError() && CollectionUtils
//                .isNotEmpty(singleQueryResult.getMatchRecords());
//    }
//
//    public static boolean checkNotEmpty(QueryResult queryResult) {
//        return Objects.nonNull(queryResult) && checkSQRNotEmpty(queryResult.getAllQueryResult());
//    }
//
//    public static boolean checkSQRNotEmpty(List<SingleQueryResult> queryResultList) {
//        if (CollectionUtils.isNotEmpty(queryResultList)) {
//            //ȫ���ɹ�����Ϊ�ɹ�
//            boolean partSuccess = false;
//            for (SingleQueryResult queryResult : queryResultList) {
//                partSuccess |= checkSQRNotEmpty(queryResult);
//            }
//            return partSuccess;
//        }
//        return false;
//    }
//
//    public static String getKey(PGQuery... query) {
//        Set<String> tables = Sets.newHashSet();
//        for (PGQuery q : query) {
//            if (q instanceof AtomicQuery) {
//                tables.add(((AtomicQuery) q).getTable());
//            }
//            if (q instanceof JoinQuery) {
//                q.toString();
//                if (q.getLeftmostAtomicQuery() != null) {
//                    tables.add(q.getLeftmostAtomicQuery().getTable() + "|join");
//                }
//            }
//        }
//        return StringUtils.join(tables, "T");
//    }
//
//    public static String toString(PGQuery... query) {
//        StringBuilder sb = new StringBuilder();
//        for (PGQuery q : query) {
//            sb.append(q.toString()).append(" ");
//        }
//        return sb.toString();
//    }
//
//    public static boolean isEmpty(QueryResult rs) {
//        return Objects.isNull(rs) || Objects.isNull(rs.getAllQueryResult()) || CollectionUtils.isEmpty(
//                rs.getAllQueryResult());
//    }
//
//    public static void writeRtToTT(long cost, PGQuery... query) {
//        if (ThreadLocalUtils.isWriteIGraphRtToTt()) {
//            String content = CommonMethods.getRtTTRecord(ConstantsFrame.TYPE_IGRAPH, getKey(query), cost);
//            CommonMethods.writeWithDiscard(ConstantsFrame.RT_TT_TOPIC, ConstantsFrame.RT_TT_ACCESS_KEY, content);
//        }
//    }
//}
