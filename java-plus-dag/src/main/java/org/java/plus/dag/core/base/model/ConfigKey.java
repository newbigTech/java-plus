package org.java.plus.dag.core.base.model;

import org.java.plus.dag.core.base.constants.StringPool;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * @author seven.wxy
 * @date 2018/12/20
 */
@Data
public class ConfigKey {
    private String configKey;
    private String configKeyPrefix;

    public ConfigKey(String configKey, String configKeyPrefix) {
        this.configKey = configKey;
        this.configKeyPrefix = configKeyPrefix;
    }

    @Override
    public String toString() {
        if (hasPrefix() && StringUtils.isNotEmpty(configKey)) {
            StringBuilder builder = new StringBuilder();
            builder.append(configKeyPrefix).append(StringPool.HASH);
            builder.append(configKey);
            return builder.toString();
        } else {
            return configKey;
        }
    }

    public boolean hasPrefix() {
        return StringUtils.isNotEmpty(configKeyPrefix);
    }
}
