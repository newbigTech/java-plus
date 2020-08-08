package org.java.plus.dag.core.ds;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
//import com.taobao.igraph.client.model.KeyList;
//import com.taobao.recommendplatform.protocol.datasource.igraph.TppDsUpdateQuery;
//import com.taobao.recommendplatform.protocol.service.ServiceFactory;
import org.java.plus.dag.core.base.constants.TppCounterNames;
import org.java.plus.dag.core.base.em.FieldNameEnum;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.EnumUtil;
import org.java.plus.dag.core.ds.model.DataSourceQueryKey;
import org.java.plus.dag.core.ds.model.DataSourceType;
import org.java.plus.dag.core.ds.model.IndexSearchPojo;
import org.java.plus.dag.core.ds.factory.IGraphConfigFactory;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfig;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfigPojo;
import org.java.plus.dag.core.ds.model.IGraphBatchRequest;
import org.java.plus.dag.core.ds.parser.IGraphDAO;
import org.java.plus.dag.core.ds.parser.IGraphDAOImpl;
import org.java.plus.dag.taobao.KeyList;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: IGraphDataSourceBase
 * @Package org.java.plus.dag.datasource
 * @date 2018/11/2 ����4:34
 */
public class IGraphDataSourceBase {
    private IGraphDataSourceConfig iGraphDataSourceConfig;
    private IGraphDAO iGraphDAO;
 
    public void setUp(ProcessorConfig processorConfig) {
        this.iGraphDataSourceConfig = IGraphConfigFactory.createConfig(processorConfig);
        iGraphDAO = new IGraphDAOImpl();
    }
 
    public DataSourceType getDataSourceType() {
        return DataSourceType.IGRAPH;
    }
 
    public DataSet<Row> read(ProcessorContext context, List<KeyList> keyListList) {
        return null;//internalRead(context, replaceConfig(context).getProcessorConfig(), keyListList, null);
    }
 
    public DataSet<Row> read(ProcessorContext context, IndexSearchPojo indexSearchPojo) {
        return null;// internalRead(context, indexSearchPojo, replaceConfig(context).getProcessorConfig());
    }
 
    public DataSet<Row> read(ProcessorContext context, Map<String, List<KeyList>> keyListMap) {
        return null;// internalRead(context, replaceConfig(context).getProcessorConfig(), keyListMap);
    }
 
    public DataSet<Row> batchRead(ProcessorContext context,
                                  Map<String, List<IGraphBatchRequest>> batchKeyListMap) {
        return null;//internalBatchRead(context, replaceConfig(context).getProcessorConfig(), batchKeyListMap);
    }
 
    protected DataSet<Row> doRead(ProcessorContext context) {
        return internalRead(context, null, null, null);
    }
 
    protected Boolean doWrite(ProcessorContext context, DataSet<Row> dataSet) {
        return internalWrite(context, dataSet, null);
    }
 
    protected Boolean doWrite(ProcessorContext context, DataSet<Row> dataSet,
                              ProcessorConfig processorConfig) {
        return internalWrite(context, dataSet, processorConfig);
    }
 
    protected DataSet<Row> doRead(ProcessorContext context,
                                  ProcessorConfig replaceConfig,
                                  List<DataSourceQueryKey> keyValuePackList) {
        return null;//internalRead(context, replaceConfig, null, keyValuePackList);
    }

    private DataSet<Row> internalRead(ProcessorContext context,
                                      IndexSearchPojo indexSearchPojo,
                                      ProcessorConfig replaceConfig) {
        return iGraphDAO.retrieveDataSet(context, getIGraphConfig(replaceConfig), indexSearchPojo);
    }

    private DataSet<Row> internalRead(ProcessorContext context,
                                      ProcessorConfig replaceConfig,
                                      List<KeyList> keyLists,
                                      List<DataSourceQueryKey> keyValuePackList) {
        //����IGraph��ѯ�õ���DataSet
        //1.�ȸ���keyLists��keyValuePackList�õ�IGraph��ѯ��Ҫ��KeyList
        //2.����У�����ռλ���滻�������replaceConfigȥִ��IGraph��ѯ
        //3.���ɲ�ѯ���
        //4.����
        return null;//iGraphDAO.retrieveDataSet(context, getIGraphConfig(replaceConfig), getIGraphKeyList(keyLists, keyValuePackList));
    }

    private DataSet<Row> internalRead(ProcessorContext context,
                                      ProcessorConfig replaceConfig,
                                      Map<String, List<KeyList>> keyListMap) {
        return null;//iGraphDAO.retrieveDataSet(context, getIGraphConfig(replaceConfig), keyListMap);
    }

    private DataSet<Row> internalBatchRead(ProcessorContext context,
                                           ProcessorConfig replaceConfig,
                                           Map<String, List<IGraphBatchRequest>> keyListMap) {
        return iGraphDAO.retrieveDataSetBatch(context, getIGraphConfig(replaceConfig), keyListMap);
    }

    private Boolean internalWrite(ProcessorContext context, DataSet<Row> dataSet,
                                  ProcessorConfig processorConfig) {
        IGraphDataSourceConfig configReplaced = IGraphConfigFactory.createConfig(processorConfig);
        String tableName = StringUtils.EMPTY;
        try {
            tableName = configReplaced.getSingleConfig().getTableName();
//            ServiceFactory.getIGraphService().batchUpdate(genUpdateList4Write(dataSet, configReplaced.getSingleConfig()));
            return true;
        } catch (Exception e) {
//            ServiceFactory.getTPPCounter().countSum(TppCounterNames.IGRAPH_UPDATE_ERROR.getCounterName() + "_" + tableName, 1);
        }
        return false;
    }

//    private List<TppDsUpdateQuery> genUpdateList4Write(DataSet<Row> dataSet, IGraphDataSourceConfigPojo configPojo) {
//        List<TppDsUpdateQuery> updateQueryList = new ArrayList<>();
//        List<Row> dataList = dataSet.getData();
//        dataList.forEach(i -> {
//            TppDsUpdateQuery updateInfo = new TppDsUpdateQuery(configPojo.getTableName(),
//                i.getFieldValue(StringUtils.isNotEmpty(configPojo.getDefaultFieldClass())
//                    ? EnumUtil.getEnumByDefault(configPojo.getDefaultFieldClass(), configPojo.getPkey())
//                    : EnumUtil.getEnum(configPojo.getPkey())),
//                genValueMap4Write(i, configPojo));
//            updateQueryList.add(updateInfo);
//        });
//        return updateQueryList;
//    }

    private Map<String, String> genValueMap4Write(Row row, IGraphDataSourceConfigPojo iGraphDataSourceConfigPojo) {
        Map<String, String> valueMap = Maps.newHashMap();
        for (String field : iGraphDataSourceConfigPojo.getIGraphQueryField()) {
            String mappingField = iGraphDataSourceConfigPojo.getFieldMapping().getOrDefault(field, field);
            String fieldValue = row.getFieldValue(getFieldEnum(mappingField, iGraphDataSourceConfigPojo));
            if (fieldValue != null) {
                valueMap.put(field, fieldValue);
            }
        }
        return valueMap;
    }

    private static FieldNameEnum getFieldEnum(String field, IGraphDataSourceConfigPojo configPojo) {
        return StringUtils.isNotEmpty(configPojo.getDefaultFieldClass())
            ? EnumUtil.getEnumByDefault(configPojo.getDefaultFieldClass(), field)
            : EnumUtil.getEnum(field);
    }

    private List<KeyList> getIGraphKeyList(List<KeyList> keyLists, List<DataSourceQueryKey> keyValuePackList) {
        List<KeyList> iGraphKeyList=null;;
        if (CollectionUtils.isNotEmpty(keyLists)) {
            iGraphKeyList = keyLists;
        } else if (CollectionUtils.isNotEmpty(keyValuePackList)) {
//            iGraphKeyList = keyValuePackList.stream().map(DataSourceQueryKey::getKeyList)
//                .filter(Objects::nonNull).collect(Collectors.toList());
        } else {
            iGraphKeyList = Lists.newArrayList();
        }
        return iGraphKeyList;
    }

    private IGraphDataSourceConfig getIGraphConfig(ProcessorConfig replaceConfig) {
        return Objects.nonNull(replaceConfig) ? IGraphConfigFactory.createConfig(replaceConfig) : this.iGraphDataSourceConfig;
    }

}
