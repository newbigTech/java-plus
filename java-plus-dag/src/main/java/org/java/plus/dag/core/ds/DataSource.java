package org.java.plus.dag.core.ds;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.proc.Processor;
import org.java.plus.dag.core.ds.model.DataSourceType;

/**
 * @author seven.wxy
 * @date 2018/9/25
 */
public interface DataSource extends Processor {
    /**
     * read data from datasource
     *
     * @param processorContext tpp url parameters
     * @return
     */
    DataSet<Row> read(ProcessorContext processorContext);

    /**
     * read data from datasource
     *
     * @param processorContext tpp url parameters
     * @param dataSet          dataSet Used for read
     * @return
     */
    DataSet<Row> read(ProcessorContext processorContext, DataSet<Row> dataSet);

    /**
     * write data to datasource
     *
     * @param processorContext
     * @param dataSet
     * @return
     */
    Boolean write(ProcessorContext processorContext, DataSet<Row> dataSet);

    /**
     * get datasource type
     *
     * @return DataSourceType
     * @see DataSourceType
     */
    default DataSourceType getDataSourceType() {
        return DataSourceType.EXTENDS;
    }
}