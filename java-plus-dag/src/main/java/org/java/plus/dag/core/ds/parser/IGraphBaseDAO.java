package org.java.plus.dag.core.ds.parser;

//import com.taobao.igraph.client.model.KeyList;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.ds.model.IGraphBatchRequest;
import org.java.plus.dag.core.ds.model.IndexSearchPojo;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfig;

import java.util.List;
import java.util.Map;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: IGraphBaseDAO
 * @Package org.java.plus.dag.frame.ds.parser
 * @date 2018/11/28 3:51 PM
 */
public abstract class IGraphBaseDAO implements IGraphDAO {

    protected static final String TPP_TABLE_PREFIX = "TPP_";

    @Override
    public DataSet<Row> retrieveDataSet(ProcessorContext context, IGraphDataSourceConfig iGraphDataSourceConfig,
                                        IndexSearchPojo indexSearchPojoList) {
        return retrieveDataSet(context, iGraphDataSourceConfig, new IndexSearchTypeHandler(indexSearchPojoList));
    }

//    @Override
//    public DataSet<Row> retrieveDataSet(ProcessorContext context, IGraphDataSourceConfig iGraphDataSourceConfig, List<KeyList> keyLists) {
//        return retrieveDataSet(context, iGraphDataSourceConfig, KeyListTypeHandler.from(keyLists));
//    }

//    @Override
//    public DataSet<Row> retrieveDataSet(ProcessorContext context, IGraphDataSourceConfig iGraphDataSourceConfig,
//          @Override
//    public DataSet<Row> retrieveDataSet(ProcessorContext context, IGraphDataSourceConfig iGraphDataSourceConfig,
//                                        Map<String, List<KeyList>> keyListMap) {
//        return retrieveDataSet(context, iGraphDataSourceConfig, new MutilKeyListTypeHandler(keyListMap));
//    }                               Map<String, List<KeyList>> keyListMap) {
//        return retrieveDataSet(context, iGraphDataSourceConfig, new MutilKeyListTypeHandler(keyListMap));
//    }

    @Override
    public DataSet<Row> retrieveDataSetBatch(ProcessorContext context, IGraphDataSourceConfig iGraphDataSourceConfig,
                                             Map<String, List<IGraphBatchRequest>> batchKeyListMap) {
        return retrieveDataSet(context, iGraphDataSourceConfig, new BatchKeyListTypeHandler(batchKeyListMap));
    }

    protected DataSet<Row> retrieveDataSet(ProcessorContext context, IGraphDataSourceConfig iGraphDataSourceConfig,
                                           IGraphTypeHandler typeHandler) {
        //�õ�Ҫ��������� �ֱ��� ͬ�����첽��������Join
        return IGraphDAOFactory
            .createHandler(iGraphDataSourceConfig)
            .retrieveProcess(context, typeHandler);
    }
}
