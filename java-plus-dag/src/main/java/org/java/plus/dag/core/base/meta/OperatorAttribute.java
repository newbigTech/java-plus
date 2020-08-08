package org.java.plus.dag.core.base.meta;

import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author seven.wxy
 * @date 2019/9/19
 */
@Data
@Accessors(chain = true)
public class OperatorAttribute {
    private String name;
    private String description;
    private String tip;
    private String type;
    @SerializedName("default")
    private Object defaultValue;
    private Long minimum;
    private Long maximum;
    private List<String> required;
    private Map<String, Object> properties;
    @SerializedName("enum")
    private List<String> enumList;
    @SerializedName("json_config")
    private OperatorAttribute jsonConfig;
}
