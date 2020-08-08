package org.java.plus.dag.core.ds.parser;

//import com.taobao.igraph.client.model.KeyList;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.ds.model.IGraphBatchRequest;
import org.java.plus.dag.core.ds.model.IGraphRequest;
import org.java.plus.dag.core.ds.model.IGraphResponse;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfig;
import org.java.plus.dag.core.ds.model.IndexSearchPojo;
import org.java.plus.dag.core.base.model.Row;

import java.util.List;
import java.util.Map;

public interface IGraphDAO {

    DataSet<Row> retrieveDataSet(ProcessorContext context, IGraphDataSourceConfig iGraphDataSource, IndexSearchPojo indexSearchPojoList);

//    DataSet<Row> retrieveDataSet(ProcessorContext context, IGraphDataSourceConfig iGraphDataSource, List<KeyList> keyLists);
//
//    DataSet<Row> retrieveDataSet(ProcessorContext context, IGraphDataSourceConfig iGraphDataSource, Map<String, List<KeyList>> keyLists);

    DataSet<Row> retrieveDataSetBatch(ProcessorContext context, IGraphDataSourceConfig iGraphDataSource,
                                      Map<String, List<IGraphBatchRequest>> keyLists);

    IGraphResponse retrieveAsync(IGraphDataSourceConfig iGraphDataSourceConfig, IGraphRequest iGraphRequest);

    IGraphResponse retrieve(IGraphDataSourceConfig iGraphDataSourceConfig, IGraphRequest iGraphRequest);
}
