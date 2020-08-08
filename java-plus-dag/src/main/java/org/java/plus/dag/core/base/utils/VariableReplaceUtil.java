package org.java.plus.dag.core.base.utils;

import com.google.common.collect.Maps;
import org.java.plus.dag.core.base.em.FieldNameEnum;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

/**
 * DESCRIPTION:
 *
 * @author jaye
 * @date 2019/10/30
 */
public class VariableReplaceUtil {
    private static final String VAR_START_FLAG = "${";
    private static final String VAR_END_FLAG = "}";
    private static final String DATA_SET_PREFIX = "DataSet.";
    private static final String DEFAULT_VAL_FLAG = ":";

    private static boolean hasVarToReplace(String str, int fromIdx) {
        int varStart = str.indexOf(VAR_START_FLAG, fromIdx);
        int varEnd = str.indexOf(VAR_END_FLAG, fromIdx);
        return (-1 != varStart && -1 != varEnd && varStart + VAR_START_FLAG.length() < varEnd);
    }

    private static List<String> getVarNameAndDefaultVal(String str, int varStart, int varEnd) {
        varStart += VAR_START_FLAG.length();
        varEnd -= VAR_END_FLAG.length();
        String variable = str.substring(varStart, varEnd);
        int idx = variable.indexOf(DEFAULT_VAL_FLAG);
        List<String> result = new ArrayList<>();
        if (0 < idx && idx < variable.length()) {
            result.add(variable.substring(0, idx));
            result.add(variable.substring(idx + 1));
        } else {
            result.add(variable);
            result.add(null);
        }
        return result;
    }

    private static boolean replaceByContext(StringBuilder resultStr, ProcessorContext context, String varName) {
        Object val = context.getTppContext().get(varName);
        if (null == val) {
            val = context.getContextData().get(varName);
        }
        if (null == val) {
            return false;
        }
        resultStr.append(val.toString());
        return true;
    }

    private static String varNameToFieldName(String varName) {
        int idx = varName.indexOf(DATA_SET_PREFIX);
        if (-1 == idx) {
            return "";
        }
        return varName.substring(idx + DATA_SET_PREFIX.length());
    }

    private static boolean replaceByDataSet(StringBuilder resultStr, DataSet<Row> dataSet, String varName) {
        String fieldName = varNameToFieldName(varName);
        StringBuilder strFromDataSet = new StringBuilder();
        dataSet.forEach(row -> {
            Object val = null;
            FieldNameEnum field = EnumUtil.getEnum(fieldName);
            if (field != null) {
                val = row.getFieldValue(field);
            }
            if (val != null) {
                strFromDataSet.append(val.toString()).append(",");
            }
        });
        if (strFromDataSet.length() > 0) { strFromDataSet.setLength(strFromDataSet.length() - 1); }
        if (strFromDataSet.length() <= 0) {
            return false;
        }
        resultStr.append(strFromDataSet.toString());
        return true;
    }


    public static String replacePlaceHolder(String originStr) {
        return replacePlaceHolder(originStr, null, null);
    }

    public static String replacePlaceHolder(String originStr, ProcessorContext context) {
        return replacePlaceHolder(originStr, context, null);
    }

    public static String replacePlaceHolder(String originStr, DataSet<Row> dataSet) {
        return replacePlaceHolder(originStr, null, dataSet);
    }

    public static String replacePlaceHolder(String originStr, ProcessorContext context,
                                            DataSet<Row> dataSet) {
        StringBuilder resultStr = new StringBuilder(originStr.length());
        int scannedIdx = 0;
        for (;scannedIdx < originStr.length() && hasVarToReplace(originStr, scannedIdx);) {
            int varStart = originStr.indexOf(VAR_START_FLAG, scannedIdx);
            int varEnd = originStr.indexOf(VAR_END_FLAG, scannedIdx) + VAR_END_FLAG.length();
            List<String> varNameAndDefaultVal = getVarNameAndDefaultVal(originStr, varStart, varEnd);
            String varName = varNameAndDefaultVal.get(0);
            String defaultVal = varNameAndDefaultVal.get(1);
            if (varStart > scannedIdx) {
                resultStr.append(originStr, scannedIdx, varStart);
            }
            boolean addVarOk = addVarFromContextAndDataSet(resultStr, context, dataSet, varName);
            if (!addVarOk) {
                addVarFromDefaultVal(resultStr, defaultVal, originStr, varStart, varEnd);
            }
            scannedIdx = varEnd;
        }
        if (scannedIdx < originStr.length()) {
            resultStr.append(originStr, scannedIdx, originStr.length());
        }
        return resultStr.toString();
    }

    private static boolean addVarFromContextAndDataSet(StringBuilder resultStr, ProcessorContext context,
                                                       DataSet<Row> dataSet, String varName) {
        boolean replaceOk = false;
        if (context != null) {
            replaceOk = replaceByContext(resultStr, context, varName);
        }
        if (!replaceOk && dataSet != null) {
            replaceOk = replaceByDataSet(resultStr, dataSet, varName);
        }
        return replaceOk;
    }

    private static void addVarFromDefaultVal(StringBuilder resultStr, String defaultVal, String originStr,
                                             int varStart, int varEnd) {
        if (defaultVal != null) {
            resultStr.append(defaultVal);
        } else {
            resultStr.append(originStr, varStart, varEnd);
        }
    }

    public static Map<String, String> parseExtraParams(ProcessorContext context, DataSet<Row> dataSet, JSONObject extraParams) {
        if (MapUtils.isEmpty(extraParams)) {
            return Maps.newHashMap();
        }
        Map<String, String> result = new HashMap<>(extraParams.size());
        extraParams.forEach((k, v) -> result.put(k, VariableReplaceUtil.replacePlaceHolder(v.toString(), context, dataSet)));
        return result;
    }
}