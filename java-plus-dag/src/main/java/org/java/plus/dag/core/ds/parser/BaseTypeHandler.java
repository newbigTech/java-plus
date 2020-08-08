package org.java.plus.dag.core.ds.parser;

import com.google.common.collect.Maps;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfigPojo;
import org.java.plus.dag.core.ds.model.IGraphRequest;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfig;
//import org.java.plus.dag.core.base.utils.igraph.IGraphQueryBuilder;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseTypeHandler implements IGraphTypeHandler {
    abstract IGraphRequest genIGraphRequestWithConfig(IGraphDataSourceConfig iGraphDataSourceConfig);

    IGraphRequest genIGraphRequestWithKeyList(IGraphDataSourceConfig iGraphDataSourceConfig) {
        throw new IllegalArgumentException("typeHandler which use keyList, need override genIGraphRequestWithKeyList");
    }

    boolean needUseKeyList() {
        return false;
    }

    void validateRequest() {
    }

    // TODO: 2019-02-02  BatchKeyListTypeHandler MutilKeyListTypeHandler ��ʱδ֧��hash
//    protected IGraphRequest genIGraphRequestByConfig(IGraphDataSourceConfig iGraphDataSourceConfig) {
//        return IGraphRequest.from(
//                iGraphDataSourceConfig.getAllConfig()
//                        .entrySet()
//                        .stream()
//                        .map(e -> IGraphQueryBuilder.createTppQuery(e.getValue(), e.getValue().getKeyLists()))
//                        .collect(Collectors.toList()));
//    }

    @Override
    public IGraphRequest getIGraphRequest(IGraphDataSourceConfig iGraphDataSourceConfig) {
        validateRequest();
        IGraphRequest request = processWhenOrElse(iGraphDataSourceConfig, needUseKeyList());
        if (iGraphDataSourceConfig.getSingleConfig().getHashTableNum() > 0) {
            Map<String, IGraphDataSourceConfigPojo> allConfig = iGraphDataSourceConfig.getAllConfig().entrySet().stream()
                .collect(Collectors.toMap(
                    e -> e.getValue().getTableName(),
                    Map.Entry::getValue,
                    (v1, v2) -> v2,
                    LinkedHashMap::new));
            iGraphDataSourceConfig.getAllConfig().clear();
            iGraphDataSourceConfig.getAllConfig().putAll(allConfig);
        }
        return request;
    }

    public IGraphRequest processWhenOrElse(IGraphDataSourceConfig iGraphDataSourceConfig, boolean useKeyList) {
        if (useKeyList) {
            return genIGraphRequestWithKeyList(iGraphDataSourceConfig);
        } else {
            return genIGraphRequestWithConfig(iGraphDataSourceConfig);
        }
    }


}
