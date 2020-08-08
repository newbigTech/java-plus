package org.java.plus.dag.core.base.meta;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author seven.wxy
 * @date 2019/9/19
 */
@Data
@Accessors(chain = true)
public class OperatorMeta {
    private static final Map INPUT_MAP;
    private static final Map OUTPUT_MAP;
    private static final Map ERROR_MAP;

    static {
        INPUT_MAP = Maps.newHashMapWithExpectedSize(2);
        OUTPUT_MAP = Maps.newHashMapWithExpectedSize(2);
        ERROR_MAP = Maps.newHashMapWithExpectedSize(1);
        INPUT_MAP.put("description", "输入数据集");
        INPUT_MAP.put("class_name", "c.t.r.s.y.c.b.m.DataSet");
        OUTPUT_MAP.put("description", "输出数据集");
        OUTPUT_MAP.put("class_name", "c.t.r.s.y.c.b.m.DataSet");
        ERROR_MAP.put("description", "算子输出异常");
    }

    private String name;
    private String description;
    private String tip;
    @SerializedName("input_arg")
    private List<Map> inputArg = Lists.newArrayList(INPUT_MAP);
    @SerializedName("output_arg")
    private List<Map> outputArg = Lists.newArrayList(OUTPUT_MAP);
    @SerializedName("input_count")
    private Integer inputCount = -1;
    @SerializedName("output_count")
    private Integer outputCount = -1;
    private String category = "rec_engine";
    private List<String> tags = Lists.newArrayList("service");
    @SerializedName("error_output_arg")
    private Map errorOutputArg = ERROR_MAP;
    private OperatorAttribute attrs;
}
