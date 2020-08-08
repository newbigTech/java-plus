package org.java.plus.dag.core.ds.parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
//import com.taobao.igraph.client.model.KeyList;
//import com.taobao.recommendplatform.protocol.datasource.igraph.TppDsAtomicQuery;
//import org.java.plus.dag.core.base.utils.igraph.IGraphQueryBuilder;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfig;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfigPojo;
import org.java.plus.dag.core.ds.model.IGraphRequest;
import org.java.plus.dag.taobao.KeyList;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: KeyListTypeHandler
 * @Package org.java.plus.dag.frame.ds.parser
 * @date 2018/11/30 11:32 AM
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class KeyListTypeHandler extends BaseTypeHandler {

    private List<KeyList> keyLists;
    //��keylist��Ϊ��ʱ ָ�����ص�count��
    private int count;

    public KeyListTypeHandler(List<KeyList> keyList, int count) {
        this.keyLists = keyList;
        this.count = count;
    }

    public static KeyListTypeHandler from(List<KeyList> keyList) {
        if (CollectionUtils.isEmpty(keyList)) {
            return new KeyListTypeHandler(new ArrayList<>(), 0);
        }
        //Ϊȷ�����ص����ݲ���ʧ����keylist����skeyʱ����Ҫָ��count��
        return new KeyListTypeHandler(keyList,
                keyList.stream().map(KeyListTypeHandler::getKeyListCount).reduce(0, (sum, num) -> sum + num));
    }

    private static int getKeyListCount(KeyList keyList) {
        return ArrayUtils.isNotEmpty(keyList.getSkeys()) ? keyList.getSkeys().length : 1;
    }

//    @Override
//    IGraphRequest genIGraphRequestWithKeyList(IGraphDataSourceConfig iGraphDataSourceConfig) {
//        List<TppDsAtomicQuery> pgQueryList = buildBatchQueryList(iGraphDataSourceConfig.getSingleConfig(), keyLists);
//        return IGraphRequest.from(pgQueryList);
//    }

//    @Override
//    IGraphRequest genIGraphRequestWithConfig(IGraphDataSourceConfig iGraphDataSourceConfig) {
//        //���ȼ� �ӿ�keyList���� �������ɵ�KeyList
//        //validate
//        List<TppDsAtomicQuery> pgQueryList = iGraphDataSourceConfig.getAllConfig()
//                .entrySet()
//                .stream()
//                .filter(e -> e.getValue().getKeyLists() != null)
//                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (k1, k2) -> k1, LinkedHashMap::new))
//                .entrySet()
//                .stream()
//                .map(e -> buildBatchQueryList(e.getValue(), e.getValue().getKeyLists()))
//                .flatMap(Collection::stream)
//                .collect(Collectors.toList());
//        if (CollectionUtils.isEmpty(pgQueryList)) {
//            throw new IllegalArgumentException("query failed, key list is empty");
//        }
//        return IGraphRequest.from(pgQueryList);
//    }

    @Override
    boolean needUseKeyList() {
        return CollectionUtils.isNotEmpty(keyLists);
    }

//    private List<TppDsAtomicQuery> buildBatchQueryList(IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo,
//                                                       List<KeyList> keyLists) {
//
//        //validate
//        if (CollectionUtils.isNotEmpty(keyLists)) {
//            iGraphDataSourceConfigPojo.setCount(Math.max(count, Math.max(keyLists.size(), iGraphDataSourceConfigPojo.getCount())));
//        }
//        //sharding by pKey
//        Map<String, List<KeyList>> pKeyHashMap = shardingBypKey(iGraphDataSourceConfigPojo, keyLists);
//        //transfer to tpp query list
//        if (MapUtils.isEmpty(pKeyHashMap)) {
//            return getBatchList(iGraphDataSourceConfigPojo, keyLists)
//                    .stream()
//                    .map(list -> IGraphQueryBuilder.createTppQuery(iGraphDataSourceConfigPojo, list))
//                    .collect(Collectors.toList());
//        }
//        return pKeyHashMap.entrySet().stream()
//                .map(e ->
//                        getBatchList(iGraphDataSourceConfigPojo, e.getValue())
//                                .stream()
//                                .map(list -> IGraphQueryBuilder.createTppQuery(iGraphDataSourceConfigPojo, list, e.getKey()))
//                                .collect(Collectors.toList()))
//                .flatMap(Collection::stream)
//                .collect(Collectors.toList());
//    }
//
//    private Map<String, List<KeyList>> shardingBypKey(IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo,
//                                                      List<KeyList> keyLists) {
//        int hashNum = iGraphDataSourceConfigPojo.getHashTableNum();
//        if (hashNum > 0) {
//            Map<String, List<KeyList>> result = keyLists.stream()
//                    .collect(Collectors.groupingBy(i -> Math.abs(i.getPkey().hashCode() % hashNum)))
//                    .entrySet()
//                    .stream()
//                    .collect(Collectors.toMap(
//                            e -> iGraphDataSourceConfigPojo.getTableName()
//                                    + iGraphDataSourceConfigPojo.getShardTableSuffixConnector() + e.getKey(),
//                            Map.Entry::getValue,
//                            (v1, v2) -> v2,
//                            LinkedHashMap::new));
//            if (MapUtils.isNotEmpty(result)) {
//                iGraphDataSourceConfigPojo.setTableName(result.keySet().iterator().next());
//            }
//            return result;
//        }
//        return new LinkedHashMap<>();
//    }

    private List<List<KeyList>> getBatchList(IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo,
                                             List<KeyList> keyLists) {
        List<List<KeyList>> batchList = new ArrayList<>();
        //batch���عرյ���� ֱ�ӷ���
        if (!iGraphDataSourceConfigPojo.isBatch()) {
            batchList.add(keyLists);
            return batchList;
        }
        //ʹ����ֵ�����
        if (iGraphDataSourceConfigPojo.useBatchThreshold()) {
            batchList = Lists.partition(keyLists, iGraphDataSourceConfigPojo.getBatchThreshold());
            return batchList;
        }
        //��ʹ����ֵʱ���������ξ���
        if (iGraphDataSourceConfigPojo.getBatchSize() > 0) {
            batchList = Lists.partition(keyLists, (int) Math.ceil(keyLists.size() * 1.0 / iGraphDataSourceConfigPojo.getBatchSize()));
        }
        return batchList;
    }

	@Override
	IGraphRequest genIGraphRequestWithConfig(IGraphDataSourceConfig iGraphDataSourceConfig) {
		// TODO Auto-generated method stub
		return null;
	}

}
