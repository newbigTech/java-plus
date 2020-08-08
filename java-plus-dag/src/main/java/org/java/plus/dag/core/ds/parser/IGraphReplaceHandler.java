package org.java.plus.dag.core.ds.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;
import org.java.plus.dag.core.ds.model.KeyPair;
import org.apache.commons.collections4.CollectionUtils;

public class IGraphReplaceHandler implements ReplaceHandler {
    public static final Splitter COMMA_SPLITTER = Splitter.on(",");

    @Override
    public List<KeyPair> replace(List<Map<String, Object>> dataSetReplaceResult) {
        if (CollectionUtils.isEmpty(dataSetReplaceResult)) {
            return new ArrayList<>();
        }
        //ͨ��List<Map<String, Object>> ����key Values ����
        //list Ϊ���н����ÿ�н�� Ϊ $.pkey �� �滻���ֵ
        //ȡ����������Ҫ��pKey �� sKey ����
        // TODO: 2019-01-15 pKey sKey�ǿɱ仯�� ��Ҫ��������޸� ����չ
        return dataSetReplaceResult.stream().map(dataMap -> {
            KeyPair keyPair = null;
            for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
                if (entry.getKey().contains("pkey")) {
                    keyPair = KeyPair.from((String) entry.getValue());
                } else if (entry.getKey().contains("skey")) {
                    List<String> sKey = COMMA_SPLITTER.splitToList((String) entry.getValue());
                    keyPair = KeyPair.from(sKey);
                }
            }
            return keyPair;
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }


}
