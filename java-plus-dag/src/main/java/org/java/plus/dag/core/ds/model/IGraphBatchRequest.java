package org.java.plus.dag.core.ds.model;

//import com.taobao.igraph.client.model.KeyList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.List;

import org.java.plus.dag.taobao.KeyList;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: IGraphBatchRequest
 * @Package org.java.plus.dag.frame.ds.parser
 * @date 2018/12/18 12:50 PM
 */
@Data
@Builder
@AllArgsConstructor
public class IGraphBatchRequest {

    private List<KeyList> keyListList;
    private int count;

}
