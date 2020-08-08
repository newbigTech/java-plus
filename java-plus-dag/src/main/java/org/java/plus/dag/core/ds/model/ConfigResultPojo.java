package org.java.plus.dag.core.ds.model;

import com.google.common.collect.Lists;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

@Data
public class ConfigResultPojo {
    //������ֵ����Դ�� ��Ӧ�� ��ֵ�����б�
    private List<DataSourceQueryKey> dataSourceQueryKeyList;
    //TPPƽ̨�е�ĳ��Processor������
    private ProcessorConfig processorConfig;

    public ConfigResultPojo() {
    }

    public ConfigResultPojo(
            List<DataSourceQueryKey> dataSourceQueryKeyList,
            ProcessorConfig processorConfig) {
        this.dataSourceQueryKeyList = dataSourceQueryKeyList;
        this.processorConfig = processorConfig;
    }

    public ConfigResultPojo(
            List<DataSourceQueryKey> dataSourceQueryKeyList) {
        this.dataSourceQueryKeyList = dataSourceQueryKeyList;
    }

    public boolean noData() {
        return CollectionUtils.isEmpty(dataSourceQueryKeyList) && processorConfig == null;
    }

    public static ConfigResultPojo from(ProcessorConfig processorConfig) {
        return new ConfigResultPojo(Lists.newArrayList(), processorConfig);
    }
}