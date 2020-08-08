package org.java.plus.dag.core.ds.parser;

import java.util.Objects;

import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfig;
import org.java.plus.dag.core.base.utils.Debugger;

/**
 * IGraphConfigFactory concrete factory.
 */
public class IGraphDAOFactory {

    public static IGraphRequestHandler createHandler(IGraphDataSourceConfig config) {
        // TODO: 2018/12/14 判断那种类型创建那种类型的config
        if (Objects.nonNull(config.getSingleConfig().getDbsConfig())) {
            return new DbsRequestHandler(config);
        } else {
            if (config.isAsync() && !Debugger.isLocal()) {
                return new AsyncRequestHandler(config);
            } else {
                return new SyncRequestHandler(config);
            }
        }
    }
}
