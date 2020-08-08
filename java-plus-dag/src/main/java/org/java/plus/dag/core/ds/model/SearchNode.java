package org.java.plus.dag.core.ds.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SearchNode {
    //����������ֵ
    private String fieldName;
    private String fieldValue;
    //��ѯ����
    // TODO: 2018/11/13 ��Ϊö��
    private String searchType = "match";

    public SearchNode(String fieldName, String fieldValue) {
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
    }
}