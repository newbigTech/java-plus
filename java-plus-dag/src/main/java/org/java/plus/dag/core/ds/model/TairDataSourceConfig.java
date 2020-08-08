package org.java.plus.dag.core.ds.model;

import org.java.plus.dag.core.base.model.ParameterConfig;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.utils.JsonUtils;
import lombok.Data;
import lombok.Getter;

import java.util.Objects;

/**
 * @author seven.wxy
 * @date 2018/9/25
 */
@Data
public class TairDataSourceConfig {
    public enum TairParameter implements ParameterConfig {
        TAIR_USER_NAME("userName", "de73ddd85ffb4c80", "tair user name"),
        TAIR_UNIT("unit", "zbyk", "tair unit"),
        TAIR_NAMESPACE("nameSpace", "148", "tair namespace"),
        TAIR_TIMEOUT("timeout", "100", "tair client timeout"),
        TAIR_PKEY("pkey", "", "search primary key"),
        TAIR_SKEY("skey", "", "search secondary key"),
        TAIR_VALUE_FIELD("field_name", "", "the field name in tair with specific query. eq: field1,field2"),
        TAIR_EXPIRE("expire", "1500", "time to live"),
        DELIMITER("delimiter", ",", "field delimiter");

        TairParameter(String name, Object defaultValue, String desc) {
            this.name = name;
            this.defaultValue = defaultValue;
            this.desc = desc;
        }

        @Getter
        private String name;
        @Getter
        private Object defaultValue;
        @Getter
        private String desc;
    }

    private String userName;
    private int timeOut;
    private int nameSpace;
    private int expire;
    private String pKey;
    private String sKey;
    private String unit;
    private String fieldName;

    private TairDataSourceConfig(String userName, int timeOut, int nameSpace, String unit,
                                String pKey, String sKey, String fieldName, int expire) {
        this.userName = userName;
        this.timeOut = timeOut;
        this.nameSpace = nameSpace;
        this.unit = unit;
        this.pKey = pKey;
        this.sKey = sKey;
        this.expire = expire;
        this.fieldName = fieldName;
    }

    public TairDataSourceConfig(String userName, int timeOut, int nameSpace, String unit) {
        this(userName, timeOut, nameSpace, unit, String.valueOf(TairParameter.TAIR_PKEY.defaultValue), String.valueOf(TairParameter.TAIR_SKEY.defaultValue), String
                .valueOf(TairParameter.TAIR_VALUE_FIELD.defaultValue), Integer
                .parseInt(TairParameter.TAIR_EXPIRE.defaultValue.toString()));
    }

    public static TairDataSourceConfig from(ProcessorConfig processorConfig) throws IllegalArgumentException {
        try {
            Objects.requireNonNull(processorConfig);
            String userName = JsonUtils.getParam(processorConfig, TairParameter.TAIR_USER_NAME);
            int timeout = Integer.parseInt(JsonUtils.getParam(processorConfig, TairParameter.TAIR_TIMEOUT));
            int namespace = Integer.parseInt(JsonUtils.getParam(processorConfig, TairParameter.TAIR_NAMESPACE));
            String pKey = JsonUtils.getParam(processorConfig, TairParameter.TAIR_PKEY);
            String sKey = JsonUtils.getParam(processorConfig, TairParameter.TAIR_SKEY);
            String tairUnit = JsonUtils.getParam(processorConfig, TairParameter.TAIR_UNIT);
            int expire = Integer.parseInt(JsonUtils.getParam(processorConfig, TairParameter.TAIR_EXPIRE));
            String fieldName = JsonUtils.getParam(processorConfig, TairParameter.TAIR_VALUE_FIELD);
            return new TairDataSourceConfig(userName, timeout, namespace, tairUnit, pKey, sKey, fieldName, expire);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

}
