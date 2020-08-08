package org.java.plus.dag.core.ds.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

//import com.alibaba.common.lang.diagnostic.Profiler;
import com.alibaba.fastjson.JSONArray;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
//import com.taobao.igraph.client.config.PgConfig;
//import com.taobao.igraph.client.model.AtomicQuery;
//import com.taobao.igraph.client.model.PGQuery;
//import com.taobao.igraph.client.model.QueryResult;
//import com.taobao.igraph.client.model.SingleQueryResult;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.constants.TppCounterNames;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.Debugger;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.base.utils.ThreadLocalUtils;
//import org.java.plus.dag.core.base.utils.igraph.IGraphUtil;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfig;
import org.java.plus.dag.core.ds.model.IGraphAsyncObject;
import org.java.plus.dag.core.ds.model.IGraphBatchQueryResult;
import org.java.plus.dag.core.ds.model.IGraphRequest;
import org.java.plus.dag.core.ds.model.IGraphResponse;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

public class IGraphDAOImpl extends IGraphBaseDAO {
    @Override
    public IGraphResponse retrieve(IGraphDataSourceConfig iGraphDataSourceConfig, IGraphRequest iGraphRequest) {
        if (iGraphRequest.isEmptyRequest()) {
            Logger.warn(() -> "iGraph request query is empty");
            return new IGraphResponse(new IGraphBatchQueryResult(Maps.newHashMap()));
        }
        return null;//batchQuery(iGraphDataSourceConfig, iGraphRequest.getPGQueryArray());
    }
//
//    @Override
//    public IGraphResponse retrieveAsync(IGraphDataSourceConfig iGraphDataSourceConfig, IGraphRequest iGraphRequest) {
//        if (iGraphRequest.isEmptyRequest()) {
//            Logger.warn(() -> "iGraph request query is empty");
//            return new IGraphResponse(new IGraphAsyncObject());
//        }
//        PGQuery[] pgQueries = iGraphRequest.getPGQueryArray();
//        PgConfig pgConfig = new PgConfig();
//        pgConfig.setRequestTimeoutInMs(iGraphDataSourceConfig.getSingleConfig().getTimeout());
//        String exceptionKey = TppCounterNames.IGRAPH_SEARCH_ASYNC_ERROR.getCounterName();
//        Debugger.put(this, () -> "iGraph_async_url", () -> StringUtils.join(pgQueries, StringPool.COMMA));
//        boolean enableProfiler = Debugger.isEnableProfiler();
//        Profiler.Entry profileEntry = null;
//        if (enableProfiler) {
//            Profiler.enter(String.format("TableName(%s),methodName(%s): ", iGraphDataSourceConfig.getSingleConfig().getTableName(), "searchAsync"));
//            profileEntry = ThreadLocalUtils.getCurrentEntry();
//        }
//        final Profiler.Entry finalProfileEntry = profileEntry;
//        long start = System.currentTimeMillis();
//        Future<QueryResult> queryResultFuture = IGraphUtil.searchAsync(exceptionKey, pgConfig, pgQueries);
//        if (Objects.isNull(queryResultFuture)) {
//            String tables = IGraphUtil.getKey(pgQueries);
//            String counterKey = TppCounterNames.IGRAPH_ASYNC_FUTURE_NULL_ERROR.getCounterName() + tables;
//            ServiceFactory.getTPPCounter().countSum(counterKey, 1);
//            Logger.onlineWarn(counterKey);
//            return new IGraphResponse(new IGraphAsyncObject());
//        }
//
//        final Function<Object, List<Row>> transformOnCompleteFunction = (response) -> {
//            List<Row> rowList = Lists.newArrayList();
//            try {
//                Future<QueryResult> res = (Future<QueryResult>)response;
//                if (Objects.nonNull(res)) {
//                    QueryResult msg = res.get(iGraphDataSourceConfig.getSingleConfig().getTimeout(), TimeUnit.MILLISECONDS);
//                    IGraphUtil.writeRtToTT((System.currentTimeMillis() - start), pgQueries);
//                    if (Objects.nonNull(msg)) {
//                        IGraphResponse iGraphResponse = getAllMatchRecord(msg, pgQueries);
//                        rowList = iGraphResponse.getResponseRowList(iGraphDataSourceConfig);
//                    }
//                }
//            } catch (Exception e) {
//                String tables = IGraphUtil.getKey(pgQueries);
//                String counterKey = TppCounterNames.IGRAPH_ASYNC_RESULT_TRANSFORM_ERROR.getCounterName() + tables;
//                ServiceFactory.getTPPCounter().countSum(counterKey, 1);
//                Logger.warn(() -> counterKey + e.getMessage());
//            } finally {
//                if (enableProfiler) {
//                    ThreadLocalUtils.releaseEntry(finalProfileEntry);
//                }
//            }
//            return rowList;
//        };
//        return new IGraphResponse(new IGraphAsyncObject(queryResultFuture, transformOnCompleteFunction));
//    }
//
//    private IGraphResponse batchQuery(IGraphDataSourceConfig iGraphDataSourceConfig, PGQuery... querys) {
//        String exceptionKey = TppCounterNames.IGRAPH_QUERY_EXCEPTION_BATCH.getCounterName();
//        PgConfig pgConfig = new PgConfig();
//        if (Debugger.isLocal()) {
//            pgConfig.setRequestTimeoutInMs(ConstantsFrame.PROCESSOR_DEBUG_TIMEOUT_MS);
//        } else {
//            pgConfig.setRequestTimeoutInMs(iGraphDataSourceConfig.getSingleConfig().getTimeout());
//        }
//        Debugger.put(this, () -> "iGraph_url", () -> StringUtils.join(querys, StringPool.COMMA));
//        boolean enableProfiler = Debugger.isEnableProfiler();
//        if (enableProfiler) {
//            Profiler.enter(String.format("TableName(%s),methodName(%s): ", iGraphDataSourceConfig.getSingleConfig().getTableName(), "search"));
//        }
//        QueryResult queryResult;
//        try {
//            if (Debugger.isLocal()) {
//                Logger.info(() -> "Request iGraph:\n" + StringUtils.join(querys, ","));
//            }
//            queryResult = IGraphUtil.search(exceptionKey, pgConfig, querys);
//        } finally {
//            if (enableProfiler) {
//                Profiler.release();
//            }
//        }
//        return getAllMatchRecord(queryResult, querys);
//    }
//
//    private IGraphResponse getAllMatchRecord(QueryResult queryResult, PGQuery... requestQuery) {
//        if (!IGraphUtil.checkNotEmpty(queryResult)) {
//            ServiceFactory.getTPPCounter().countSum(TppCounterNames.IGRAPH_RESULT_NULL.getCounterName() + IGraphUtil.getKey(requestQuery), 1);
//            return new IGraphResponse();
//        }
//        if (ArrayUtils.isEmpty(requestQuery)) {
//            Logger.warn(() -> "IGraph request query empty");
//            ServiceFactory.getTPPCounter().countSum(TppCounterNames.IGRAPH_REQUEST_EMPTY.getCounterName(), 1);
//            return new IGraphResponse();
//        }
//        return new IGraphResponse(getBatchQueryResponse(queryResult.getAllQueryResult(), requestQuery));
//    }
//
//    private IGraphBatchQueryResult getBatchQueryResponse(List<SingleQueryResult> queryResultList,
//                                                         PGQuery... requestQuery) {
//        IGraphSingleQueryResult.IGraphSingleQueryResultBuilder singleBuilder = IGraphSingleQueryResult.builder();
//        List<IGraphSingleQueryResult> singleQueryResultList = new ArrayList<>();
//        for (int i = 0; i < queryResultList.size(); ++i) {
//            String tableName = ((AtomicQuery)requestQuery[i]).getTable();
//            if (tableName.startsWith(TPP_TABLE_PREFIX)) {
//                tableName = tableName.substring(TPP_TABLE_PREFIX.length());
//            }
//            singleBuilder.queryIndex(i);
//            singleBuilder.queryResult(queryResultList.get(i).getMatchRecords());
//            singleBuilder.tableName(tableName);
//            singleQueryResultList.add(singleBuilder.build());
//        }
//        return IGraphBatchQueryResult.aggregate(singleQueryResultList);
//    }

	@Override
	public IGraphResponse retrieveAsync(IGraphDataSourceConfig iGraphDataSourceConfig, IGraphRequest iGraphRequest) {
		// TODO Auto-generated method stub
		return null;
	}
}
