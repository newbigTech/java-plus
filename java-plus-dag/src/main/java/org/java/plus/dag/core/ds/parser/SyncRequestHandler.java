package org.java.plus.dag.core.ds.parser;

import java.util.List;

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
 * @Title: SyncRequestHandler
 * @Package org.java.plus.dag.frame.ds.parser
 * @date 2018/12/19 12:29 AM
 */
public class SyncRequestHandler implements IGraphRequestHandler {
    @Getter
    @Setter
    private IGraphDataSourceConfig iGraphDataSourceConfig;
    private IGraphDAO iGraphDAO;

    public SyncRequestHandler(IGraphDataSourceConfig iGraphDataSourceConfig) {
        this.iGraphDataSourceConfig = iGraphDataSourceConfig;
        this.iGraphDAO = new IGraphDAOImpl();
    }

    @Override
    public DataSet<Row> retrieveProcess(ProcessorContext context, IGraphTypeHandler typeHandler) {
        DataSet<Row> rowDataSet = new DataSet<>();
        IGraphRequest iGraphRequest = typeHandler.getIGraphRequest(iGraphDataSourceConfig);
        IGraphResponse iGraphResponse = iGraphDAO.retrieve(iGraphDataSourceConfig, iGraphRequest);
        List<Row> rowList = iGraphResponse.getResponseRowList(iGraphDataSourceConfig);
        rowDataSet.setData(rowList);
        rowDataSet.setSource(iGraphDataSourceConfig.getRequestTableString());
        return rowDataSet;
    }

}
