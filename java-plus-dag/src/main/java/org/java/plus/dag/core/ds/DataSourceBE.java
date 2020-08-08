package org.java.plus.dag.core.ds;

import java.util.Map;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;

public interface DataSourceBE extends DataSource {

    /**
     * read data from datasource
     *
     * @param processorContext tpp url parameters
     * @param paramMap         url extend parameters
     * @return DataSet<Row>
     */
    DataSet<Row> read(ProcessorContext processorContext, Map<String, String> paramMap);

}
