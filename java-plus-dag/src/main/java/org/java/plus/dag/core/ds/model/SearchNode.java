package org.java.plus.dag.core.ds.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SearchNode {
    //列名，具体值
    private String fieldName;
    private String fieldValue;
    //查询类型
    // TODO: 2018/11/13 改为枚举
    private String searchType = "match";

    public SearchNode(String fieldName, String fieldValue) {
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
    }
}