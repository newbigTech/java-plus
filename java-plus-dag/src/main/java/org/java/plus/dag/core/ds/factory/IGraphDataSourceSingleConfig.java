package org.java.plus.dag.core.ds.factory;

import com.google.common.base.Joiner;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import org.apache.commons.collections4.MapUtils;

import java.util.*;

/**
 * @author seven.wxy
 * @date 2018/9/25
 */
@Data
@AllArgsConstructor
public class IGraphDataSourceSingleConfig implements IGraphDataSourceConfig {

    private Map<String, IGraphDataSourceConfigPojo> iGraphDataSourceConfig = new LinkedHashMap<>();
    private boolean async;
    private static final Joiner TABLE_NAME_JOINER = Joiner.on(",");

    @Override
    public IGraphDataSourceConfigPojo getSingleConfig() {
        if (MapUtils.isNotEmpty(iGraphDataSourceConfig)) {
            return iGraphDataSourceConfig.values().stream().findFirst().get();
        }
        return null;
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

    public static IGraphDataSourceSingleConfig from(@NonNull ProcessorConfig processorConfig)
            throws IllegalArgumentException {
        try {
            IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo = IGraphDataSourceUtil.from(processorConfig);
            Map<String, IGraphDataSourceConfigPojo> iGraphDataSourceConfigMap = new HashMap<>();
            iGraphDataSourceConfigMap.put(iGraphDataSourceConfigPojo.getTableName(), iGraphDataSourceConfigPojo);
            boolean async = iGraphDataSourceConfigPojo.isAsync();
            return new IGraphDataSourceSingleConfig(iGraphDataSourceConfigMap, async);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
