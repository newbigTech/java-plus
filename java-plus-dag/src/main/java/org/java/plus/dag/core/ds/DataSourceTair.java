package org.java.plus.dag.core.ds;

import java.util.List;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.tair.ILdbDataMerge;

/**
 * Tair DataSource external interface
 */
public interface DataSourceTair extends DataSource {
    /**
     * read data from Tair with pKey list
     *
     * @param processorContext
     * @param pKeys
     * @return
     */
    DataSet<Row> read(ProcessorContext processorContext, List<String> pKeys);

    /**
     * read data from Tari with pKey and sKey
     *
     * @param processorContext
     * @param pKey
     * @param sKey
     * @return
     */
    DataSet<Row> read(ProcessorContext processorContext, String pKey, String sKey);

    /**
     * write data to tair with multi vertion merge strategy and default expire time 0
     *
     * @param processorContext
     * @param dataSet
     * @param merge
     * @return
     */
    boolean writeWithVersion(ProcessorContext processorContext, DataSet<Row> dataSet, ILdbDataMerge merge);

    /**
     * write data to tair with multi vertion merge strategy
     *
     * @param processorContext
     * @param dataSet
     * @param merge
     * @param expireTime
     * @return
     */
    boolean writeWithVersion(ProcessorContext processorContext, DataSet<Row> dataSet, ILdbDataMerge merge,
                             int expireTime);

    /**
     * write data with version check, if version check error, return immediately
     * @param context
     * @param dataSet
     * @param expireTime
     * @return
     */
    boolean writeWithVersionCheck(ProcessorContext context, DataSet<Row> dataSet, int expireTime);

    boolean writeWithOutVersionCheck(DataSet<Row> dataSet, int expireTime);

    /**
     * delete data from Tair with pKey list
     *
     * @param processorContext
     * @param pKeys
     * @return
     */
    boolean delete(ProcessorContext processorContext, List<String> pKeys);

}
