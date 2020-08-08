package org.java.plus.dag.core.ds.model;

import org.java.plus.dag.core.ds.parser.IGraphSingleQueryResult;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: IGraphBatchQueryResult
 * @Package org.java.plus.dag.frame.ds.parser
 * @date 2018/12/16 6:00 PM
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class IGraphBatchQueryResult {

    private Map<String, List<IGraphSingleQueryResult>> batchResult;

    public static IGraphBatchQueryResult aggregate(List<IGraphSingleQueryResult> singleQueryResultList) {
        if (CollectionUtils.isEmpty(singleQueryResultList)) {
            return new IGraphBatchQueryResult();
        }
        return new IGraphBatchQueryResult(singleQueryResultList.stream().collect(
            Collectors.groupingBy(IGraphSingleQueryResult::getTableName)));
    }

    public boolean isEmpty() {
        return MapUtils.isEmpty(batchResult);
    }
}
