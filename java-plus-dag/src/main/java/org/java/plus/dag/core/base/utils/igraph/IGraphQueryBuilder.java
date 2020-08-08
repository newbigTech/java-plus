//package org.java.plus.dag.core.base.utils.igraph;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import java.util.stream.Collectors;
//
//import com.google.common.collect.Lists;
//import com.taobao.igraph.client.model.AtomicQuery;
//import com.taobao.igraph.client.model.JoinQuery;
//import com.taobao.igraph.client.model.KeyList;
//import com.taobao.igraph.client.model.PGQuery;
//import com.taobao.igraph.client.model.QueryResult;
//import com.taobao.igraph.client.model.SingleQueryResult;
//import com.taobao.recommendplatform.protocol.datasource.igraph.TppDsAtomicQuery;
//import com.taobao.recommendplatform.protocol.service.IGraphService;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
//import org.java.plus.dag.core.base.utils.Logger;
//import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfigPojo;
//import org.apache.commons.collections4.CollectionUtils;
//import org.apache.commons.lang3.StringUtils;
//
///**
// * Created by admin on 2016/12/2. query build toolkit
// */
//public class IGraphQueryBuilder {
//    public static final SingleQueryResult dummySingleQueryResult = new SingleQueryResult();
//    public static final QueryResult dummyQueryResult = new QueryResult() {
//        {
//            setResults(Collections.singletonList(dummySingleQueryResult));
//        }
//    };
//    public static IGraphService iGraphService = ServiceFactory.getIGraphService();
//
//    public static JoinQuery createJoinQuery(PGQuery leftQuery, AtomicQuery rightQuery,
//                                            String joinField) {
//        return createJoinQuery(leftQuery, rightQuery, joinField, null, 0, 0);
//    }
//
//    public static JoinQuery createJoinQuery(PGQuery leftQuery, AtomicQuery rightQuery,
//                                            String joinField, int start,
//                                            int count) {
//        return createJoinQuery(leftQuery, rightQuery, joinField, null, start, count);
//    }
//
//    public static JoinQuery createJoinQuery(PGQuery leftQuery, AtomicQuery rightQuery,
//                                            String joinField,
//                                            String sortField, int start, int count) {
//        if (leftQuery == null || rightQuery == null || joinField == null) {
//            return null;
//        }
//        JoinQuery joinQuery = new JoinQuery(leftQuery, rightQuery, joinField);
//        if (start >= 0 && count > 0) {
//            joinQuery.setRange(start, count);
//        }
//        if (sortField != null) {
//            joinQuery.setOrderby(sortField);
//        }
//
//        return joinQuery;
//    }
//
//    public static TppDsAtomicQuery createTppDimQuery(String dataSource, String id, String aliasId) {
//        TppDsAtomicQuery query = new TppDsAtomicQuery(dataSource);
//        if (id != null && aliasId != null) {
//            Map<String, String> aliasMap = new HashMap<>();
//            aliasMap.put(id, aliasId);
//            query.setAlias(aliasMap);
//        }
//        return query;
//    }
//
//    public static TppDsAtomicQuery createTppQuery(String dataSource, String pkey) {
//        return createTppQuery(dataSource, pkey, null);
//    }
//
//    public static TppDsAtomicQuery createTppQuery(String dataSource, String pkey, String[] fields) {
//        return createTppQuery(dataSource, pkey, null, fields);
//    }
//
//    public static TppDsAtomicQuery createTppQuery(String dataSource, String pkey, String sortField,
//                                                  String[] fields) {
//        return createTppQuery(dataSource, pkey, sortField, -1, -1, fields);
//    }
//
//    public static TppDsAtomicQuery createTppQuery(String dataSource, String pkey, String sortField,
//                                                  int start,
//                                                  int count, String[] fields) {
//        return createTppQuery(dataSource, pkey, null, sortField, start, count, fields);
//    }
//
//    public static TppDsAtomicQuery createTppQuery(String dataSource, String pkey, String[] skeys,
//                                                  String sortField,
//                                                  int start, int count, String[] fields) {
//        return createTppQuery(dataSource, pkey, skeys, start, count, null, sortField, fields, 0, StringUtils.EMPTY);
//    }
//
//    public static TppDsAtomicQuery createTppQuery(String dataSource, String pKey, String[] sKeys,
//                                                  int start, int count, String filter,
//                                                  String orderBy, String[] fields, int localCount, String distinct) {
//        if (StringUtils.isEmpty(StringUtils.trimToEmpty(pKey))) {
//            return null;
//        }
//        TppDsAtomicQuery query;
//        if (sKeys != null && sKeys.length > 0) {
//            KeyList keyList = new KeyList(pKey, sKeys);
//            query = new TppDsAtomicQuery(dataSource, keyList);
//        } else {
//            query = new TppDsAtomicQuery(dataSource, new KeyList(pKey));
//        }
//        if (StringUtils.isNotEmpty(StringUtils.trimToEmpty(filter))) {
//            query.setFilter(filter);
//        }
//        if (StringUtils.isNotEmpty(StringUtils.trimToEmpty(orderBy))) {
//            query.setOrderby(orderBy);
//        }
//        if (start >= 0 && count > 0) {
//            query.setRange(start, count);
//        }
//        if (fields != null && fields.length > 0) {
//            query.setReturnFields(fields);
//        }
//        if (localCount > 0) {
//            query.setLocalCount(localCount);
//        }
//        if (StringUtils.isNotEmpty(distinct)) {
//            query.setDistinct(distinct);
//        }
//        return query;
//    }
//
//    public static TppDsAtomicQuery createPkeysTppQuery(String dataSource, List<KeyList> pKeys,
//                                                       int count, String filter, String orderBy, String[] fields) {
//        return createPkeysTppQuery(dataSource, pKeys, count, filter, orderBy, fields, 0, StringUtils.EMPTY);
//    }
//
//    public static TppDsAtomicQuery createPkeysTppQuery(String dataSource, List<KeyList> pKeys,
//                                                       int count, String filter, String orderBy, String[] fields,
//                                                       int localCount, String distinct) {
//        TppDsAtomicQuery query = new TppDsAtomicQuery(dataSource, pKeys);
//        if (count > 0) {
//            query.setRange(0, count);
//        }
//        if (filter != null && filter.length() > 0) {
//            query.setFilter(filter);
//        }
//        if (orderBy != null && orderBy.length() > 0) {
//            query.setOrderby(orderBy);
//        }
//        if (fields != null && fields.length > 0) {
//            query.setReturnFields(fields);
//        }
//        if (localCount > 0) {
//            query.setLocalCount(localCount);
//        }
//        if (StringUtils.isNotEmpty(distinct)) {
//            query.setDistinct(distinct);
//        }
//        return query;
//    }
//
//    public static QueryResult retrySearch(String key, PGQuery... queries) {
//        long start = System.currentTimeMillis();
//        QueryResult result = dummyQueryResult;
//        for (int i = 0; i < 2; i++) {
//            try {
//                result = iGraphService.search(queries);
//            } catch (Exception e) {
//                Logger.error("[recall]", e);
//            }
//            if (result != null) {
//                break;
//            }
//        }
//        long cost = System.currentTimeMillis() - start;
//        IGraphUtil.writeRtToTT(cost, queries);
//        return result;
//    }
//
//    public static QueryResult retrySearch(PGQuery... queries) {
//        long start = System.currentTimeMillis();
//        QueryResult result = dummyQueryResult;
//        for (int i = 0; i < 2; i++) {
//            try {
//                result = iGraphService.search(queries);
//            } catch (Exception e) {
//                Logger.error("[recall]", e);
//            }
//            if (result != null) {
//                break;
//            }
//        }
//        long cost = System.currentTimeMillis() - start;
//        IGraphUtil.writeRtToTT(cost, queries);
//        return result;
//    }
//
//    public static boolean checkSQRNotEmpty(SingleQueryResult singleQueryResult) {
//        return !Objects.isNull(singleQueryResult) && !singleQueryResult.hasError() && CollectionUtils
//                .isNotEmpty(singleQueryResult.getMatchRecords());
//    }
//
//    public static boolean isEmpty(QueryResult rs) {
//        return rs == null || null == rs.getAllQueryResult() || rs.getAllQueryResult().size() == 0
//                || null == rs.getQueryResult(0)
//                || null == rs.getQueryResult(0).getMatchRecords()
//                || 0 == rs.getQueryResult(0).getMatchRecords().size();
//    }
//
//    public static TppDsAtomicQuery createTppQuery(IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo) {
//        return createTppQuery(iGraphDataSourceConfigPojo, new ArrayList<>());
//    }
//
//    public static TppDsAtomicQuery createTppQuery(IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo,
//                                                  List<KeyList> keyLists) {
//        return createTppQuery(iGraphDataSourceConfigPojo, keyLists, iGraphDataSourceConfigPojo.getCount());
//    }
//
//    public static TppDsAtomicQuery createTppQuery(IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo,
//                                                  List<KeyList> keyLists, String tableName) {
//        return createTppQuery(iGraphDataSourceConfigPojo, keyLists, iGraphDataSourceConfigPojo.getCount(), tableName);
//    }
//
//    public static TppDsAtomicQuery createTppQuery(IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo,
//                                                  List<KeyList> keyLists, int count) {
//        return createTppQuery(iGraphDataSourceConfigPojo, keyLists, count, iGraphDataSourceConfigPojo.getTableName());
//    }
//
//    public static TppDsAtomicQuery createTppQuery(IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo,
//                                                  List<KeyList> keyLists, int count, String tableName) {
//        if (CollectionUtils.isEmpty(keyLists)) {
//            return createTppQuery(tableName,
//                    iGraphDataSourceConfigPojo.getPkey(),
//                    iGraphDataSourceConfigPojo.getIGraphSKeys(),
//                    iGraphDataSourceConfigPojo.getStart(),
//                    count,
//                    iGraphDataSourceConfigPojo.getFilter(),
//                    iGraphDataSourceConfigPojo.getOrderby(),
//                    iGraphDataSourceConfigPojo.getIGraphQueryField(),
//                    iGraphDataSourceConfigPojo.getLocalCount(),
//                    iGraphDataSourceConfigPojo.getDistinct());
//        } else {
//            return createPkeysTppQuery(tableName,
//                    keyLists,
//                    count,
//                    iGraphDataSourceConfigPojo.getFilter(),
//                    iGraphDataSourceConfigPojo.getOrderby(),
//                    iGraphDataSourceConfigPojo.getIGraphQueryField(),
//                    iGraphDataSourceConfigPojo.getLocalCount(),
//                    iGraphDataSourceConfigPojo.getDistinct());
//        }
//    }
//
//    public static List<TppDsAtomicQuery> createTppQueryBatch(IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo,
//                                                             List<KeyList> keyLists) {
//        //�������ֵ
//        if (CollectionUtils.isNotEmpty(keyLists)) {
//            iGraphDataSourceConfigPojo.setCount(Math.max(keyLists.size(), iGraphDataSourceConfigPojo.getCount()));
//        }
//
//        List<List<KeyList>> batchList = getBatchList(iGraphDataSourceConfigPojo, keyLists);
//
//        return createTppQueryWithBatchList(iGraphDataSourceConfigPojo, batchList);
//    }
//
//    public static List<TppDsAtomicQuery> createTppQueryWithBatchList(
//            IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo, List<List<KeyList>> batchList) {
//        if (CollectionUtils.isEmpty(batchList)) {
//            return new ArrayList<>();
//        }
//        return batchList
//                .stream()
//                .map(list -> IGraphQueryBuilder.createTppQuery(iGraphDataSourceConfigPojo, list))
//                .collect(Collectors.toList());
//    }
//
//    private static List<List<KeyList>> getBatchList(IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo,
//                                                    List<KeyList> keyLists) {
//        List<List<KeyList>> batchList = new ArrayList<>();
//        if (iGraphDataSourceConfigPojo.isBatch() && iGraphDataSourceConfigPojo.getBatchSize() > 0) {
//            batchList = Lists.partition(keyLists,
//                    keyLists.size() / iGraphDataSourceConfigPojo.getBatchSize());
//        } else {
//            batchList.add(keyLists);
//        }
//        return batchList;
//    }
//
//}
