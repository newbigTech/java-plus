package org.java.plus.dag.core.ds.parser;

import com.google.common.annotations.Beta;
//import com.taobao.recommendplatform.protocol.datasource.igraph.TppDsAtomicQuery;
import org.java.plus.dag.core.ds.model.IGraphBatchRequest;
import org.java.plus.dag.core.ds.model.IGraphRequest;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfig;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfigPojo;
//import org.java.plus.dag.core.base.utils.igraph.IGraphQueryBuilder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: KeyListTypeHandler
 * @Package org.java.plus.dag.frame.ds.parser
 * @date 2018/11/30 11:32 AM
 */
@Beta
@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
public class BatchKeyListTypeHandler extends BaseTypeHandler {

    private Map<String, List<IGraphBatchRequest>> iGraphBatchRequestMap;

    @Override
    IGraphRequest genIGraphRequestWithKeyList(IGraphDataSourceConfig iGraphDataSourceConfig) {
//        List<TppDsAtomicQuery> pgQueryList = buildBatchQueryList(iGraphDataSourceConfig, iGraphBatchRequestMap);
        return null;// IGraphRequest.from(pgQueryList);
    }

    @Override
    IGraphRequest genIGraphRequestWithConfig(IGraphDataSourceConfig iGraphDataSourceConfig) {
        //���ȼ� �ӿ�keyList���� �������ɵ�KeyList
        return null;//genIGraphRequestByConfig(iGraphDataSourceConfig);
    }

    @Override
    boolean needUseKeyList() {
        return MapUtils.isNotEmpty(iGraphBatchRequestMap);
    }

//    private List<TppDsAtomicQuery> buildBatchQueryList(IGraphDataSourceConfig iGraphDataSourceConfig,
//                                                       Map<String, List<IGraphBatchRequest>> paramRequest) {
//        if (Objects.isNull(iGraphDataSourceConfig)) {
//            return new ArrayList<>();
//        }
//
//        //������������
//        Map<String, List<IGraphBatchRequest>> configRequest = getConfigKeyListMap(iGraphDataSourceConfig);
//
//        //merge configMap and paramMap
//        Map<String, List<IGraphBatchRequest>> mergeResult = merge(paramRequest, configRequest);
//
//        //generate query list
//        return generateQueryList(iGraphDataSourceConfig, mergeResult);
//    }

//    private List<TppDsAtomicQuery> generateQueryList(IGraphDataSourceConfig iGraphDataSourceConfig,
//                                                     Map<String, List<IGraphBatchRequest>> batchRequestList) {
//        return iGraphDataSourceConfig.getAllConfig()
//                .entrySet()
//                .stream()
//                .filter(e -> CollectionUtils.isNotEmpty(batchRequestList.get(e.getKey())))
//                .map(e -> createTppQuery(e.getValue(), batchRequestList.get(e.getKey())))
//                .flatMap(Collection::stream)
//                .collect(Collectors.toList());
//    }

    private Map<String, List<IGraphBatchRequest>> merge(Map<String, List<IGraphBatchRequest>> paramRequest,
                                                        Map<String, List<IGraphBatchRequest>> configRequest) {
        configRequest.putAll(paramRequest);
        return configRequest;
    }

    private Map<String, List<IGraphBatchRequest>> getConfigKeyListMap(IGraphDataSourceConfig iGraphDataSourceConfig) {
        Map<String, List<IGraphBatchRequest>> configMap = new LinkedHashMap<>();
        List<IGraphBatchRequest> iGraphBatchRequestList = new ArrayList<>();
        iGraphDataSourceConfig.getAllConfig().forEach((k, v) -> {
            IGraphBatchRequest.IGraphBatchRequestBuilder builder = IGraphBatchRequest.builder();
            if (CollectionUtils.isEmpty(v.getKeyLists())) {
                configMap.put(k, new ArrayList<>());
            } else {
                builder.count(v.getMaxCount());
                builder.keyListList(v.getKeyLists());
                iGraphBatchRequestList.add(builder.build());
                configMap.put(k, iGraphBatchRequestList);
            }
        });
        return configMap;
    }

//    private static List<TppDsAtomicQuery> createTppQuery(IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo,
//                                                         List<IGraphBatchRequest> iGraphRequestList) {
//        if (CollectionUtils.isEmpty(iGraphRequestList)) {
//            return new ArrayList<>();
//        }
//        return iGraphRequestList.stream().map(
//                i -> IGraphQueryBuilder.createTppQuery(iGraphDataSourceConfigPojo, i.getKeyListList(), i.getCount()))
//                .collect(Collectors.toList());
//    }

}
