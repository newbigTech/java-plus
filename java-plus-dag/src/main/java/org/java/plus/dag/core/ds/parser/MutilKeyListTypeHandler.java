package org.java.plus.dag.core.ds.parser;

import com.google.common.annotations.Beta;
//import com.taobao.igraph.client.model.KeyList;
//import com.taobao.recommendplatform.protocol.datasource.igraph.TppDsAtomicQuery;
import org.java.plus.dag.core.ds.model.IGraphRequest;
import org.java.plus.dag.taobao.KeyList;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfig;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfigPojo;
//import org.java.plus.dag.core.base.utils.igraph.IGraphQueryBuilder;
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
public class MutilKeyListTypeHandler extends BaseTypeHandler {

    private Map<String, List<KeyList>> keyListMap;

    public MutilKeyListTypeHandler(Map<String, List<KeyList>> keyListMap) {
        this.keyListMap = keyListMap;
    }

//    @Override
//    IGraphRequest genIGraphRequestWithKeyList(IGraphDataSourceConfig iGraphDataSourceConfig) {
//        List<TppDsAtomicQuery> pgQueryList = buildBatchQueryList(iGraphDataSourceConfig, keyListMap);
//        return IGraphRequest.from(pgQueryList);
//    }

//    @Override
//    IGraphRequest genIGraphRequestWithConfig(IGraphDataSourceConfig iGraphDataSourceConfig) {
//        return genIGraphRequestByConfig(iGraphDataSourceConfig);
//    }

    @Override
    boolean needUseKeyList() {
        return MapUtils.isNotEmpty(keyListMap);
    }

//    private List<TppDsAtomicQuery> buildBatchQueryList(IGraphDataSourceConfig iGraphDataSourceConfig,
//                                                       Map<String, List<KeyList>> paramKeyListMap) {
//        if (Objects.isNull(iGraphDataSourceConfig) || Objects.isNull(iGraphDataSourceConfig.getAllConfig())) {
//            return new ArrayList<>();
//        }
//
//        //������������
//        Map<String, List<KeyList>> configKeyListMap = getConfigKeyListMap(iGraphDataSourceConfig);
//
//        //merge configMap and paramMap
//        Map<String, List<KeyList>> mergeMap = merge(paramKeyListMap, configKeyListMap);
//
//        //alter count in map
//        mergeMap = setCountWithKeyListMap(mergeMap, iGraphDataSourceConfig);
//
//        //generate query list
//        return generateQueryList(iGraphDataSourceConfig, mergeMap);
//    }
//
//    private List<TppDsAtomicQuery> generateQueryList(IGraphDataSourceConfig iGraphDataSourceConfig,
//                                                     Map<String, List<KeyList>> finalKeyListMap) {
//        return iGraphDataSourceConfig.getAllConfig()
//                .entrySet()
//                .stream()
//                .filter(e -> CollectionUtils.isNotEmpty(finalKeyListMap.get(e.getKey())))
//                .map(e -> IGraphQueryBuilder.createTppQueryBatch(e.getValue(), finalKeyListMap.get(e.getKey())))
//                .flatMap(Collection::stream)
//                .collect(Collectors.toList());
//    }

    private Map<String, List<KeyList>> merge(Map<String, List<KeyList>> paramMap,
                                             Map<String, List<KeyList>> configMap) {
        configMap.putAll(paramMap);
        return configMap;
    }

    private Map<String, List<KeyList>> getConfigKeyListMap(IGraphDataSourceConfig iGraphDataSourceConfig) {
        Map<String, List<KeyList>> configMap = new HashMap<>();
        iGraphDataSourceConfig.getAllConfig().forEach((k, v) -> {
            if (CollectionUtils.isEmpty(v.getKeyLists())) {
                configMap.put(k, new ArrayList<>());
            } else {
                configMap.put(k, v.getKeyLists());
            }
        });
        return configMap;
    }

    private Map<String, List<KeyList>> setCountWithKeyListMap(Map<String, List<KeyList>> mergeMap,
                                                              IGraphDataSourceConfig iGraphDataSourceConfig) {
        if (MapUtils.isEmpty(mergeMap)) {
            return new LinkedHashMap<>();
        }
        mergeMap.forEach((k, v) -> {
            //��ȡĳ��������� ��������������ΪKeyList�������е����ֵ
            IGraphDataSourceConfigPojo configPojo = iGraphDataSourceConfig.getAllConfig().get(k);
            if (configPojo != null) {
                configPojo.setCount(Math.max(configPojo.getCount(), v.size()));
            }
        });
        return mergeMap;
    }

	@Override
	IGraphRequest genIGraphRequestWithConfig(IGraphDataSourceConfig iGraphDataSourceConfig) {
		// TODO Auto-generated method stub
		return null;
	}
}
