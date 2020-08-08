package org.java.plus.dag.core.ds.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: IndexSearchPojo
 * @Package org.java.plus.dag.frame.base.model
 * @date 2018/11/13 ����1:56
 */
@Data
@AllArgsConstructor
public class IndexSearchPojo {

    //����
    private IndexSearchType operation;
    private List<SearchNode> nodes;

    public int getSize() {
        if (CollectionUtils.isNotEmpty(nodes)) {
            return this.getNodes().size();
        } else {
            return 0;
        }
    }

    public String getIndexSearchString() {
        StringBuilder indexSearch;
        boolean oneItemSearch = getNodes().size() == 1;
        if (!oneItemSearch) {
            indexSearch = new StringBuilder("\\{\"" + operation.getName() + "\":[");
        } else {
            indexSearch = new StringBuilder();
        }
        int i = 0;
        for (SearchNode searchNode : nodes) {
            indexSearch.append("\\{\"").append(searchNode.getSearchType()).append("\":\\{\"").append(
                searchNode.getFieldName()).append("\":\"").append(
                searchNode.getFieldValue()).append("\"\\}\\}");
            if (i != nodes.size() - 1) {
                indexSearch.append(",");
            }
            i++;
        }
        if (!oneItemSearch) {
            indexSearch.append("]\\}");
        }
        return indexSearch.toString();
    }
}
