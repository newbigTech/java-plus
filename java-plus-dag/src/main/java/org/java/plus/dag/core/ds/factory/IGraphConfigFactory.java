package org.java.plus.dag.core.ds.factory;

import com.alibaba.fastjson.JSONArray;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.utils.JsonUtils;

/**
 * IGraphConfigFactory concrete factory.
 * @author
 */
public class IGraphConfigFactory {
    public static IGraphDataSourceConfig createConfig(ProcessorConfig processConfig) {
        JSONArray jsonArray = JsonUtils.getParam(processConfig, "multi_query", new JSONArray());
        if (jsonArray.isEmpty()) {
            return IGraphDataSourceSingleConfig.from(processConfig);
        } else {
            return IGraphDataSourceMultiConfig.from(processConfig);
        }
    }
}
