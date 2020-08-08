package org.java.plus.dag.core.ds.parser;

//import com.taobao.igraph.client.model.MatchRecord;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.List;

import org.java.plus.dag.taobao.MatchRecord;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: IGraphBatchQueryResult
 * @Package org.java.plus.dag.frame.ds.parser
 * @date 2018/12/16 6:00 PM
 */
@Data
@AllArgsConstructor
@Builder
public class IGraphSingleQueryResult {
    private List<MatchRecord> queryResult;
    private int queryIndex;
    private String tableName;
}
