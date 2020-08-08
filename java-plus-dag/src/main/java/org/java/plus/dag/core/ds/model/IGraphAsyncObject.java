package org.java.plus.dag.core.ds.model;

//import com.taobao.igraph.client.model.QueryResult;
import org.java.plus.dag.core.base.model.Row;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;

@Data
@NoArgsConstructor
public class IGraphAsyncObject {
    private Function<Object, List<Row>> asyncFunction;
    private Future<Object> asyncQuery;

    public IGraphAsyncObject(Future<Object> asyncQuery, Function<Object, List<Row>> asyncFunction) {
        this.asyncQuery = asyncQuery;
        this.asyncFunction = asyncFunction;
    }

    public Future<Object> getAsyncQuery() {
        return this.asyncQuery;
    }

    public Function<Object, List<Row>> getAsyncFunction() {
        return this.asyncFunction;
    }
}
