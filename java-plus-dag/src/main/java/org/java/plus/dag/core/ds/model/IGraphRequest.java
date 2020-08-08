package org.java.plus.dag.core.ds.model;

//import com.taobao.igraph.client.model.PGQuery;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: IGraphRequest
 * @Package org.java.plus.dag.frame.ds.parser
 * @date 2018/12/3 6:48 PM
 */
@Data
public class IGraphRequest {

    List<? extends Object> pgQueryList;

    private IGraphRequest(List<? extends Object> pgQueryList) {
        this.pgQueryList = pgQueryList;
    }

    public void setSinglePGQuery(Object pgQuery) {
        if (pgQuery != null) {
            this.pgQueryList = Arrays.asList(pgQuery);
        }
    }

    public boolean isNotValid() {
        return CollectionUtils.isEmpty(pgQueryList);
    }

    public Object[] getPGQueryArray() {
        if (CollectionUtils.isEmpty(pgQueryList)) {
            return new Object[0];
        }
        return pgQueryList.toArray(new Object[0]);
    }

    public boolean isEmptyRequest() {
        return CollectionUtils.isEmpty(pgQueryList);
    }

    public static IGraphRequest from(List<? extends Object> pgQueryList) {
        return new IGraphRequest(pgQueryList);
    }

    public static IGraphRequest from(Object pgQuery) {
        return new IGraphRequest(Arrays.asList(pgQuery));
    }

    public static IGraphRequest emptyRequest() {
        return new IGraphRequest(new ArrayList<>());
    }
}
