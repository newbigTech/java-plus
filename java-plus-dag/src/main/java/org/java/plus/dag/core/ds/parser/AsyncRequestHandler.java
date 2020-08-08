package org.java.plus.dag.core.ds.parser;

import java.util.ArrayList;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfig;
import org.java.plus.dag.core.ds.model.IGraphRequest;
import org.java.plus.dag.core.ds.model.IGraphResponse;
import lombok.Getter;
import lombok.Setter;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: AsyncRequestHandler
 * @Package org.java.plus.dag.frame.ds.parser
 * @date 2018/12/18 11:22 PM
 */
public class AsyncRequestHandler implements IGraphRequestHandler {
    @Getter
    @Setter
    private IGraphDataSourceConfig iGraphDataSourceConfig;
    private IGraphDAO iGraphDAO;

    public AsyncRequestHandler(IGraphDataSourceConfig iGraphDataSourceConfig) {
        this.iGraphDataSourceConfig = iGraphDataSourceConfig;
        this.iGraphDAO = new IGraphDAOImpl();
    }

    @Override
    public DataSet<Row> retrieveProcess(ProcessorContext context, IGraphTypeHandler typeHandler) {
        DataSet<Row> rowDataSet = new DataSet<>();

        IGraphRequest iGraphRequest = typeHandler.getIGraphRequest(iGraphDataSourceConfig);
        IGraphResponse iGraphResponse = iGraphDAO.retrieveAsync(iGraphDataSourceConfig, iGraphRequest);

        if (iGraphResponse.isAsyncEmpty()) {
            rowDataSet.setData(new ArrayList<>());
        } else {
            rowDataSet.setAsyncData(iGraphResponse.getIGraphAsyncObject().getAsyncQuery(),
                iGraphResponse.getIGraphAsyncObject().getAsyncFunction());
        }
        rowDataSet.setSource(iGraphDataSourceConfig.getRequestTableString());
        return rowDataSet;
    }

}
