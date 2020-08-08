package org.java.plus.dag.core.dataflow.example;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import org.java.plus.dag.core.base.annotation.ConfigInit;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.proc.BaseProcessor;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.base.utils.StrUtils;
import org.java.plus.dag.core.dataflow.DataFlow;
import org.java.plus.dag.core.dataflow.bizops.home_page.GetUserFeature;
import org.java.plus.dag.core.dataflow.bizops.home_page.JsonMatchTypeMap;
import org.java.plus.dag.core.dataflow.core.DataFlowCore;
import org.java.plus.dag.core.dataflow.core.Operation;
import org.java.plus.dag.core.dataflow.ops.AddPoolDataType;
import org.java.plus.dag.core.dataflow.ops.AppendTriggerTime;
import org.java.plus.dag.core.dataflow.ops.GetBeExtendTrigger;
import org.java.plus.dag.core.dataflow.ops.ParseBeRecall;
import org.apache.commons.lang3.StringUtils;

/**
 * DESCRIPTION:
 *
 * @author jaye
 * @date 2019/3/13
 */
public class BeRecall extends BaseProcessor {
    private static final String Q_INFO = "qinfo";
    private static final String RT_TAGS = "rt_tags";
    private static final String VDO_ID_A_EXTEND = "vdo_id_a_extend";

    @ConfigInit(desc = "match type to recall type mapping")
    private String matchTypeMapping = StringUtils.EMPTY;
    @ConfigInit(desc = "need user feature or not")
    private boolean needUserFeature = false;
    @ConfigInit(desc = "need time info or not")
    private boolean needTimeInfo = false;
    @ConfigInit(desc = "be DataSource config key")
    private String beKey = "Solution_datasource_be#datasource/BEDataSource$berecall";
    @ConfigInit(desc = "user feature iGraph config key")
    private String userFeatureIgraphKey = "Solution_datasource_igraph#datasource/IGraphDataSource$berecallUserFeature";
    @ConfigInit(desc = "if use user's interest tag")
    private boolean useUserInterestTag = false;
    @ConfigInit(desc = "need trigger extend or not")
    private boolean needTriggerExtend = false;
    @ConfigInit(desc = "utdid play log igraph config key")
    private String utdidVdoPlaylogKey = "Solution_datasource_igraph#datasource/IGraphDataSource$utdidVdoPlaylog";
    @ConfigInit(desc = "utdid play log igraph config key")
    private String utdidShowPlaylogKey = "Solution_datasource_igraph#datasource/IGraphDataSource$utdidShowPlaylog";
    @ConfigInit(desc = "uid play log igraph config key")
    private String uidVdoPlaylogKey = "Solution_datasource_igraph#datasource/IGraphDataSource$uidVdoPlaylog";
    @ConfigInit(desc = "need uid playLog extend or not")
    private boolean needUidPlaylogExtend = false;
    @ConfigInit(desc = "convert tag id to name adaptor")
    private String userInterestTagKey = "Solution_processor_config#service/strategy/TagIdToNameAdaptor";
    @ConfigInit(desc = "qinfo key")
    private String qinfoKey = "";
    @ConfigInit(desc = "auto mapping type by publish id")
    private boolean autoMappingType = false;
    @ConfigInit(desc = "pool data map igraph key")
    private String poolDataIgraphKey;

    private Map<Integer,String> matchTypeMap = Maps.newHashMap();

    @Override
    public void doInit(ProcessorConfig processorConfig) {
        super.doInit(processorConfig);
        matchTypeMap = new JsonMatchTypeMap(matchTypeMapping).eval(getName());
    }

    private String getUserInterestTag(DataSet<Row> dataSet){
        Set<String> tagNames = new HashSet<>();
        List<Row> rowList = dataSet.getData();
        for (Row row : rowList) {
            String tagName = row.getFieldValue(AllFieldName.tag_name);
            if (StringUtils.isNotBlank(tagName)) {
                tagNames.add(tagName);
            }
        }
        return StringUtils.join(tagNames, ",");
    }

    private Operation<Map<String, String>> genReqParam(ProcessorContext context, DataSet<Row> vdoPlaylog,
                                                       DataSet<Row> uidPlaylog, DataSet<Row> qinfo,
                                                       DataSet<Row> userInterestTag) {
        return DataFlow.newOpDependNone(ctx -> {
            Map<String, String> config = Maps.newHashMap();
            Map<String, String> qInfoMap = new HashMap<>();
            if (needUserFeature) {
                qInfoMap.putAll(new GetUserFeature(userFeatureIgraphKey, needTimeInfo).eval(
                    getName(), HashMap::new));
            }
            if (!qinfo.isEmpty()) {
                Row row = qinfo.getData().get(0);
                qInfoMap.putAll(row.getFieldValue(AllFieldName.QinfoMap));
            }
            config.put(Q_INFO, StrUtils.map2Str(qInfoMap, StringPool.COLON, StringPool.SEMICOLON));
            if (useUserInterestTag) {
                config.put(RT_TAGS, getUserInterestTag(userInterestTag));
            }
            if (needTriggerExtend) {
                DataSet<Row> playlog = vdoPlaylog;
                if (needUidPlaylogExtend && 0 != context.getUid()) {
                    playlog = playlog.unionAll(uidPlaylog);
                }
                String triggerExtend = new GetBeExtendTrigger(playlog).eval(getName());
                config.put(VDO_ID_A_EXTEND, triggerExtend);
            }
            return config;
        }, e -> {
            Logger.warn(() -> "BeRecall: genReqPram failed, " + e.getMessage());
            return new HashMap<>();
        });
    }

    @Override
    public DataSet<Row> doProcess(ProcessorContext context, DataSet<Row> mainDataSet,
                                  Map<String, DataSet<Row>> dataSetMap) {
        DataSet<Row> vdoPlaylog = getDataSetByProcessorConfigKey(utdidVdoPlaylogKey, context,
            mainDataSet, dataSetMap);
        DataSet<Row> showPlaylog = getDataSetByProcessorConfigKey(utdidShowPlaylogKey, context,
            mainDataSet, dataSetMap);
        DataSet<Row> uidPlaylog = getDataSetByProcessorConfigKey(uidVdoPlaylogKey, context, mainDataSet, dataSetMap);
        DataSet<Row> qinfo = dataSetMap.getOrDefault(qinfoKey, new DataSet<>());
        DataSet<Row> userInterestTag = dataSetMap.getOrDefault(userInterestTagKey, new DataSet<>());

        DataFlow df = DataFlowCore.getInstance(context, this);
        Operation<Map<String, String>> paramOp = genReqParam(context, vdoPlaylog, uidPlaylog,
            qinfo, userInterestTag);
        Operation<DataSet<Row>> beResult = df.readDataSource(beKey, paramOp);
        Operation<DataSet<Row>> parsedResult = new ParseBeRecall(beResult, matchTypeMap);
        Operation<DataSet<Row>> finalResult = new AppendTriggerTime(parsedResult, vdoPlaylog.unionAll(showPlaylog));
        if (autoMappingType) {
            finalResult = new AddPoolDataType(finalResult, poolDataIgraphKey);
        }
        return df.eval(finalResult);
    }
}