package org.java.plus.dag.core.ds.factory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.google.common.base.Joiner;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.utils.Debugger;
import org.java.plus.dag.core.base.utils.JsonUtils;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.base.utils.ThreadLocalUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import org.apache.commons.collections4.MapUtils;

/**
 * @author seven.wxy
 * @date 2018/9/25
 */
@Data
@AllArgsConstructor
public class IGraphDataSourceMultiConfig implements IGraphDataSourceConfig {

    private Map<String, IGraphDataSourceConfigPojo> iGraphDataSourceConfig = new LinkedHashMap<>();
    private boolean async;
    private static final Joiner TABLE_NAME_JOINER = Joiner.on(",");

    @Override
    public IGraphDataSourceConfigPojo getSingleConfig() {
        if (MapUtils.isEmpty(iGraphDataSourceConfig)) {
            Logger.warn(() -> "iGraphDataSourceConfig is empty");
            return null;
        }
        return iGraphDataSourceConfig.entrySet().iterator().next().getValue();
    }

    @Override
    public Map<String, IGraphDataSourceConfigPojo> getAllConfig() {
        return iGraphDataSourceConfig;
    }

    @Override
    public List<String> getRequestTableList() {
        if (MapUtils.isEmpty(iGraphDataSourceConfig)) {
            return new ArrayList<>();
        }
        return new ArrayList<>(iGraphDataSourceConfig.keySet());
    }

    @Override
    public String getRequestTableString() {
        return TABLE_NAME_JOINER.join(getRequestTableList());
    }

    public static IGraphDataSourceMultiConfig from(@NonNull ProcessorConfig processorConfig)
        throws IllegalArgumentException {
        try {
            JSONArray array = JsonUtils.getParam(processorConfig, "multi_query", new JSONArray());
            Map<String, IGraphDataSourceConfigPojo> iGraphDataSourceConfigMap = parseMultiArray(array);
            //��������������
            Boolean solutionIGraphAsync = ThreadLocalUtils.isAsyncIgraph();
            Boolean async = JsonUtils.getParam(processorConfig, "async", solutionIGraphAsync);
            if (Debugger.isLocal()) {
                async = false;
            }
            return new IGraphDataSourceMultiConfig(iGraphDataSourceConfigMap, async);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static Map<String, IGraphDataSourceConfigPojo> parseMultiArray(JSONArray array) {
        Map<String, IGraphDataSourceConfigPojo> iGraphDataSourceConfigMap = new LinkedHashMap<>();
        for (Object anArray : array) {
            try {
                JSONObject object = (JSONObject)anArray;
                IGraphDataSourceConfigPojo config = IGraphDataSourceUtil.from(ProcessorConfig.deepfrom(object));
                iGraphDataSourceConfigMap.put(config.getTableName(), config);
            } catch (Exception ignored) { }
        }
        return iGraphDataSourceConfigMap;
    }
}
