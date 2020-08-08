package org.java.plus.dag.core.ds.model;

//import com.taobao.recommendplatform.protocol.concurrent.AsyncResult;
//import com.taobao.recommendplatform.protocol.service.dii.DIIResponse;
import org.java.plus.dag.core.base.model.Row;
import org.springframework.scheduling.annotation.AsyncResult;

import java.util.List;
import java.util.function.Function;

public class BeAsyncObject {
    private Function<Object, List<Row>> asyncFunction;
    private AsyncResult<Object> asyncQuery;

    public BeAsyncObject(AsyncResult<Object> asyncQuery, Function<Object, List<Row>> asyncFunction) {
        this.asyncQuery = asyncQuery;
        this.asyncFunction = asyncFunction;
    }

    public AsyncResult<Object> getAsyncQuery() {
        return this.asyncQuery;
    }

    public Function<Object, List<Row>> getAsyncFunction() {
        return this.asyncFunction;
    }
}
