package org.java.plus.dag.core.ds.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSONPath;
//import com.alibaba.glaucus.protocol.solution.SolutionContext;

import com.google.common.collect.Lists;
//import com.taobao.igraph.client.model.KeyList;
//import com.taobao.recommendplatform.protocol.solution.Context;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.EnumUtil;
import org.java.plus.dag.core.base.utils.Logger;
import org.java.plus.dag.core.ds.model.ConfigResultPojo;
import org.java.plus.dag.core.ds.model.DataSourceType;
import org.java.plus.dag.core.ds.model.DataSourceOperationType;
import org.java.plus.dag.core.ds.model.DataSourceQueryKey;
import org.java.plus.dag.core.ds.model.KeyPair;
import org.java.plus.dag.core.ds.model.PlaceHolderDO;
import org.java.plus.dag.core.ds.utils.PlaceHolderUtil;
import org.java.plus.dag.solution.Context;
import org.java.plus.dag.taobao.KeyList;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrLookup;
import org.apache.commons.lang3.text.StrSubstitutor;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: ConfigParser
 * @Package org.java.plus.dag.frame.ds.parser
 * @date 2018/11/10 ����4:21
 */
public class ConfigParser {
    private static final String DATA_SET_PREFIX = "${DataSet.";
    private static final String DATA_SET_SUFFIX = "}";
    private static final String OPTIONAL_PREFIX = "$.optional";

    /**
     * ��ProcessContext �� �滻ռλ��
     *
     * @param processorContext tpp url parameters
     * @param processorConfig  tpp2.0�ж���� processConfig ���� ���Tpp�е�����
     * @param placeHolderDO    ռλ������ ��Ϊjson·����ֵΪ����json·���µ�ֵ
     * @return {@link ConfigResultPojo} ����һ�����ö������а����滻������ö���
     */
    public static ConfigResultPojo replaceToProcessConfig(final ProcessorConfig processorConfig,
                                                          ProcessorContext processorContext,
                                                          final PlaceHolderDO placeHolderDO) {
        return replaceToProcessConfig(processorConfig, processorContext, null, placeHolderDO);
    }

    /**
     * ��ProcessContext �� dataSet �滻ռλ��
     *
     * @param processorContext tpp url parameters
     * @param processorConfig  tpp2.0�ж���� processConfig ���� ���Tpp�е�����
     * @param dataSet          tpp2.0�ж���� ���ݼ�
     * @param placeHolderDO    ռλ��Map ��Ϊjson·����ֵΪ����json·���µ�ֵ
     * @return {@link ConfigResultPojo} ����һ�����ö������а������ݼ����õ��ļ�ֵ�Ժ��滻������ö���
     */

    public static ConfigResultPojo replaceToProcessConfig(final ProcessorConfig processorConfig,
                                                          ProcessorContext processorContext,
                                                          final DataSet<Row> dataSet,
                                                          final PlaceHolderDO placeHolderDO) {
        return replaceToProcessConfig(processorConfig, processorContext, dataSet, placeHolderDO,
            DataSourceOperationType.READ, DataSourceType.IGRAPH);
    }

    /**
     * ��ProcessContext �� dataSet �滻ռλ��
     *
     * @param processorContext        tpp url parameters
     * @param processorConfig         tpp2.0�ж���� processConfig ���� ���Tpp�е�����
     * @param dataSet                 tpp2.0�ж���� ���ݼ�
     * @param placeHolderDO           ռλ��Map ��Ϊjson·����ֵΪ����json·���µ�ֵ
     * @param dataSourceOperationType ����Դ�������� READ WRITE
     * @param dataSourceType          ����Դ���� ����ʹ��DataSet ��̬�滻ʱ �Ż��õ�
     * @return {@link ConfigResultPojo} ����һ�����ö������а������ݼ����õ��ļ�ֵ�Ժ��滻������ö���
     */
    public static ConfigResultPojo replaceToProcessConfig(final ProcessorConfig processorConfig,
                                                          ProcessorContext processorContext,
                                                          final DataSet<Row> dataSet,
                                                          final PlaceHolderDO placeHolderDO,
                                                          final DataSourceOperationType dataSourceOperationType,
                                                          final DataSourceType dataSourceType) {
        //���ռλ��MapΪ�� ֱ�ӷ���
        if (!validate(placeHolderDO)) {
            return ConfigResultPojo.from(processorConfig);
        }
        //��ΪprocessorConfig �� ���ȫ�ֱ���
        //���������Ϊ��ÿ�������ʱ���޸Ķ��󣬲�Ӱ���´�����
        ProcessorConfig processorConfigReplaced = ProcessorConfig.deepfrom(processorConfig);
        checkRequiredField(processorConfigReplaced);

        //�滻�̶�ռλ��Map
        boolean replaceFlag = replaceFixedHolder(processorConfigReplaced, placeHolderDO);

        //����ռλ��Map ʹ��ProcessContext�滻
        replaceFlag |= replaceHolderWithProcessContext(processorConfigReplaced, processorContext, placeHolderDO);

        ConfigResultPojo resultPojo = new ConfigResultPojo();
        if (replaceFlag) {
            resultPojo.setProcessorConfig(processorConfigReplaced);
        }
        String defaultFieldClass = processorConfigReplaced.getString("defaultFieldClass");
        //����ռλ��Map��dataSet������DataSourceQueryKey
        if (dataSet != null && dataSourceOperationType == DataSourceOperationType.READ) {
            List<DataSourceQueryKey> dataSourceQueryKeyList = getFromDataSet(placeHolderDO, dataSet, dataSourceType, defaultFieldClass);
            if (CollectionUtils.isNotEmpty(dataSourceQueryKeyList)) {
                resultPojo.setDataSourceQueryKeyList(dataSourceQueryKeyList);
            }
        }
        return resultPojo;
    }

    private static boolean checkRequiredField(ProcessorConfig processorConfig) {
        Map<String, String> requiredMap = PlaceHolderUtil.getJsonPathMap(processorConfig.get("required"));
        if (MapUtils.isNotEmpty(requiredMap)) {
            Logger.onlineWarn("context data has no value=" + requiredMap.values());
        }
        return requiredMap.size() == 0;
    }

    private static boolean replaceFixedHolder(ProcessorConfig processorConfig,
                                              PlaceHolderDO placeHolderDO) {
        Map<String, String> fixedHolderMap = placeHolderDO.getDoneMap();
        if (MapUtils.isEmpty(fixedHolderMap)) {
            return false;
        }

        Map<String, String> timestampMap = new HashMap<>();
        timestampMap.put(PlaceHolderDO.TIMESTAMP_HOLDER, String.valueOf(System.currentTimeMillis()));

        //�����滻ռλ����Map
        StrSubstitutor sub = new StrSubstitutor(timestampMap);

        fixedHolderMap.forEach((k, v) -> {
            String replaceValue = sub.replace(v);
            JSONPath.set(processorConfig, k, replaceValue);
        });
        return true;
    }

    private static List<DataSourceQueryKey> getFromDataSet(PlaceHolderDO placeHolderDO, DataSet<Row> dataSet,
                                                           DataSourceType dataSourceType, String defaultFieldClass) {
        Map<String, String> placeHolderMap = placeHolderDO.getPathHolderMap();
        //��ȡ��DataSet�滻��Ľ��
        List<Map<String, Object>> afterReplace = getKeyValueFromDataSet(placeHolderMap, dataSet.getData(), defaultFieldClass);
        if (CollectionUtils.isEmpty(afterReplace)) {
            return new ArrayList<>();
        }
        //��ȡpKey sKey�Ľ��List
        return buildKeyValueResult(afterReplace, placeHolderDO, dataSourceType);
    }

    private static boolean replaceHolderWithProcessContext(ProcessorConfig processorConfig,
                                                           ProcessorContext processorContext,
                                                           PlaceHolderDO placeHolderDO) {
        Map<String, String> placeHolderMap = placeHolderDO.getPathHolderMap();
        Map<String, Object> defaultMap = placeHolderDO.getDefaultMap();

        AtomicBoolean replaceFlag = new AtomicBoolean(false);
        placeHolderMap.forEach((k, v) -> {
            //ȥ��${}
            String valueFormatted = PlaceHolderUtil.fastRemovePlaceHolderMark(v);
            //���ȴ�tppContext���滻
            Context tppContext = processorContext.getTppContext();
            BiFunction<Context, String, Object> getTppContextFn = null;//SolutionContext::get;
            if (replaceProcessContextValue(tppContext, getTppContextFn, valueFormatted, v, k, processorConfig,
                placeHolderDO)) {
                replaceFlag.set(true);
                return;
            }

            //����չ�����滻
            Map<String, Object> contextData = processorContext.getContextData();
            BiFunction<Map<String, Object>, String, Object> getTppContextDataFn = Map::get;
            if (replaceProcessContextValue(contextData, getTppContextDataFn, valueFormatted, v, k, processorConfig,
                placeHolderDO)) {
                replaceFlag.set(true);
                return;
            }

            //ȡĬ��ֵ
            Object defaultValue = defaultMap.get(k.substring(k.lastIndexOf('.') + 1));
            if (Objects.nonNull(defaultValue)) {
                JSONPath.set(processorConfig, k, defaultValue);
                replaceFlag.set(true);
                return;
            }
            //�����optional�� ����û��Ĭ��ֵ Ӧ��ɾ��
            if (k.startsWith(OPTIONAL_PREFIX)) {
                JSONPath.remove(processorConfig, k);
            }
        });
        return replaceFlag.get();
    }

    private static List<Map<String, Object>> getKeyValueFromDataSet(Map<String, String> configDataColumnMap,
                                                                    List<Row> dataMapList, String defaultFieldClass) {
        List<Map<String, Object>> dataList = new ArrayList<>();
        if (CollectionUtils.isEmpty(dataMapList)) {
            return new ArrayList<>();
        }
        StrSubstitutor sub = new StrSubstitutor();
        //�����滻ռλ��
        //�� ĳ������ȡ�� ����ĳ�ֹ��� �滻 ���� ĳ������0
        sub.setVariablePrefix(DATA_SET_PREFIX);
        sub.setVariableSuffix(DATA_SET_SUFFIX);

        dataMapList.forEach(i -> {
            //����ÿһ��(Row)
            //�����滻������
            sub.setVariableResolver(new StrLookup<String>() {
                @Override
                public String lookup(String key) {
                    Object value = StringUtils.isNotEmpty(defaultFieldClass)
                        ? i.getFieldValue(EnumUtil.getEnumByDefault(defaultFieldClass, key))
                        : i.getFieldValue(EnumUtil.getEnum(key));
                    return Objects.isNull(value) ? null : String.valueOf(value);
                }
            });
            configDataColumnMap.entrySet().stream()
                .filter(e -> StringUtils.trimToEmpty(e.getValue()).startsWith(DATA_SET_PREFIX))
                .forEach(e -> {
                    Map<String, Object> tmpMap = new HashMap<>();
                    tmpMap.put(e.getKey(), sub.replace(e.getValue()));
                    dataList.add(tmpMap);
                });
            }
        );
        return dataList;
    }

    /**
     * ͨ��List<Map<String, Object>> ����key Values ����
     * list Ϊ���н����ÿ�н�� Ϊ $.pkey �� �滻���ֵ
     * ȡ����������Ҫ��pKey �� sKey ����
     * @param dataSetReplaceResult
     * @param placeHolderDO
     * @param dataSourceType
     * @return
     */
    private static List<DataSourceQueryKey> buildKeyValueResult(List<Map<String, Object>> dataSetReplaceResult,
                                                                PlaceHolderDO placeHolderDO,
                                                                DataSourceType dataSourceType) {
        if (CollectionUtils.isEmpty(dataSetReplaceResult) || Objects.isNull(dataSourceType)) {
            return Lists.newArrayList();
        }
        //����dataSource���� ����PKey Skey����
        //TODO ���Գ���Ϊ����ʵ����
        //��dataSourceType �� newһ��context ѡ����ʵĴ����� ������
        List<KeyPair> keyPairs = new ArrayList<>();
        switch (dataSourceType) {
            case IGRAPH:
                ReplaceHandler replaceHandler = new IGraphReplaceHandler();
                keyPairs = replaceHandler.replace(dataSetReplaceResult);
                break;
            case TAIR:
                //TODO ����Tair ����
                break;
            case BE:
                //TODO ����BE ����
                break;
            default:
                break;
        }
        return merge2QueryKey(keyPairs, placeHolderDO);
    }

    /**
     * ��ʹ��DataSet�滻��Ľ�� �� ProcessContext�滻��Ľ���ϲ� ����DataSource��ʶ���DataSourceQueryKey
     * @param dataSetKeyPair
     * @param placeHolderDO
     * @return
     */
    private static List<DataSourceQueryKey> merge2QueryKey(List<KeyPair> dataSetKeyPair, PlaceHolderDO placeHolderDO) {
        if (CollectionUtils.isEmpty(dataSetKeyPair) && MapUtils.isEmpty(placeHolderDO.getReplacedMap())) {
            return new ArrayList<>();
        }
        //case 1 һ��pKey ProcessContext ���sKey DataSet
        //case 2 ���pKey DataSet
        if (!placeHolderDO.needReplacePKeyOrSKey()) {
            return dataSetKeyPair.stream().filter(keyPair -> StringUtils.isNotEmpty(keyPair.getPkey()))
                .map(i -> new DataSourceQueryKey(new KeyList(i.getPkey())))
                .collect(Collectors.toList());
        }
        Map<String, List<String>> pKeysKeyMap = new LinkedHashMap<>();
        dataSetKeyPair.forEach(keyPair ->
            pKeysKeyMap.computeIfAbsent(placeHolderDO.getReplacePKey(), k -> new ArrayList<>()).addAll(keyPair.getSkeys())
        );
        return pKeysKeyMap.entrySet().stream()
            .map(e -> new DataSourceQueryKey(new KeyList(e.getKey(), e.getValue().toArray(new String[0]))))
            .collect(Collectors.toList());
    }

    private static boolean validate(PlaceHolderDO placeHolderDO) {
        return placeHolderDO != null && !placeHolderDO.isEmpty();
    }

    private static <T, R> boolean replaceProcessContextValue(T context, BiFunction<T, String, R> getDataFn,
                                                             String replaceValue, String originValue, String path,
                                                             ProcessorConfig processorConfig,
                                                             PlaceHolderDO placeHolderDO) {
        if (placeHolderDO == null) {
            return false;
        }
        AtomicBoolean replaceFlag = new AtomicBoolean(false);
        Optional.ofNullable(context)
            .ifPresent(i -> Optional.ofNullable(getDataFn.apply(context, replaceValue))
                .ifPresent(value -> {
                    String finalValue = StringUtils.replace(originValue, "${" + replaceValue + "}",
                        String.valueOf(value));
                    JSONPath.set(processorConfig, path, finalValue);
                    //��¼�滻��·�� �� ֵ�� ����ͬһ·����ֻ����һ��ֵ������ֱ��put�Ϳ���
                    placeHolderDO.getReplacedMap().put(path, finalValue);
                    replaceFlag.set(true);
                }));
        return replaceFlag.get();
    }
}
