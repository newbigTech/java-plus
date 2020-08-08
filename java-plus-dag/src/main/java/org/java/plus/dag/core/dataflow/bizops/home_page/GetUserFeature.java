package org.java.plus.dag.core.dataflow.bizops.home_page;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

import org.java.plus.dag.core.base.em.AllFieldName;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.CalendarUtil;
import org.java.plus.dag.core.base.utils.StrUtils;
import org.java.plus.dag.core.dataflow.core.Operation;
import org.java.plus.dag.core.dataflow.ops.ReadDataSource;
import org.apache.commons.lang3.StringUtils;

/**
 * DESCRIPTION:
 *
 * @author jaye
 * @date 2019/3/13
 */
public class GetUserFeature extends Operation<Map<String,String>> {
    private static final String DEFAULT_INT = "0";
    private static final String WEEK_DAY = "week_day";
    private static final String HOUR_TIME = "hour_time";

    private String userFeatureIgraphKey;
    private boolean needTimeInfo;

    public GetUserFeature(String userFeatureIgraphKey) { this.userFeatureIgraphKey = userFeatureIgraphKey; }

    public GetUserFeature(String userFeatureIgraphKey, boolean needTimeInfo) {
        this.userFeatureIgraphKey = userFeatureIgraphKey;
        this.needTimeInfo = needTimeInfo;
    }

    @Override
    public Map<String, String> apply(ProcessorContext ctx) {
        Map<String, String> qInfoMap = new HashMap<>();
        DataSet<Row> featureRes = new ReadDataSource(userFeatureIgraphKey).eval(getCurProcessorName());
        List<Row> rowList = featureRes.getData();
        if (!rowList.isEmpty()) {
            Row row = rowList.get(0);
            String qInfo = row.getFieldValue(AllFieldName.fea_json);
            if (StringUtils.isNotEmpty(qInfo)) {
                JSONObject json = JSONObject.parseObject(qInfo);
                for (String featureName : json.keySet()) {
                    String featureValue = json.getString(featureName);
                    qInfoMap.put(featureName, StrUtils.getOrDefault(featureValue, DEFAULT_INT));
                }
            }
        }
        if (needTimeInfo) {
            qInfoMap.put(WEEK_DAY, String.valueOf(CalendarUtil.getDayOfWeek()));
            qInfoMap.put(HOUR_TIME, String.valueOf(CalendarUtil.getHour(CalendarUtil.getCurrentDate())));
        }
        return qInfoMap;
    }
}