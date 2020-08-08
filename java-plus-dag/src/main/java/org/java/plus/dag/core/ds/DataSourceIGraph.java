package org.java.plus.dag.core.ds;

//import com.taobao.igraph.client.model.KeyList;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.ds.model.IndexSearchPojo;
import org.java.plus.dag.taobao.KeyList;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.ds.model.IGraphBatchRequest;

import java.util.List;
import java.util.Map;

public interface DataSourceIGraph extends DataSource {

    /**
     * read data from datasource
     *
     * @param processorContext tpp url parameters
     * @param keyLists igraph key list which will be use in query
     * @return
     */
    DataSet<Row> read(ProcessorContext processorContext, List<KeyList> keyLists);

    /**
     * read data from datasource
     *
     * @param processorContext tpp url parameters
     * @param keyListMap use for multi query, key=igraph config's table_name, value=keylist use for query
     * @link https://yuque.antfin-inc.com/aone638902/tpp2.0/syh383
     * @return
     */
    DataSet<Row> read(ProcessorContext processorContext, Map<String, List<KeyList>> keyListMap);

    /**
     * read data from datasource
     *
     * @param processorContext tpp url parameters
     * @param batchListMap use for batch query, key=igraph config's table_name, value=batch structure use for query
     * @link https://yuque.antfin-inc.com/aone638902/tpp2.0/syh383
     * @return
     */
    DataSet<Row> batchRead(ProcessorContext processorContext, Map<String, List<IGraphBatchRequest>> batchListMap);

    /**
     * read data from datasource
     *
     * @param processorContext tpp url parameters
     * @param indexSearchPojo  index query
     * @return
     */
    DataSet<Row> read(ProcessorContext processorContext, IndexSearchPojo indexSearchPojo);
}
