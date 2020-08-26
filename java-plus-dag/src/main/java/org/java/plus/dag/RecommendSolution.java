package org.java.plus.dag;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.java.plus.dag.core.base.constants.StringPool;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.proc.Processor;
import org.java.plus.dag.core.base.utils.DataParseUtils;
import org.java.plus.dag.core.base.utils.SolutionConfig;
import org.java.plus.dag.core.base.utils.StrUtils;
import org.java.plus.dag.core.base.utils.TppConfigUtil;
import org.java.plus.dag.core.base.utils.TppObjectFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 场景推荐入口
 * */
public class RecommendSolution {

    public  List<? extends Object> doRecommend(ProcessorContext context) {
        List<? extends Object> objects = Lists.newArrayList();
        try {
            List<Processor> processors = initProcessors(context);
            Map<String, DataSet<Row>> resultMap = this.executeProcessorChain(context, processors);
            DataSet<Row> result = resultMap.get(ConstantsFrame.MAIN_CHAIN_KEY);
            objects = processResult(context, result);
        } catch (Exception e) {
//            ServiceFactory.getSolutionLogger().error(e.getMessage(), e);
        }
        return objects;
    }
    

    public   List<Processor> initProcessors(ProcessorContext context) {
//        List<String> processorNames = getConfig(SolutionConfig.SOLUTION_PROCESSOR.getName());
        List<String> processorNames = getConfig("content");
        return getProcessorList(processorNames, context);
    }

    private   List<Processor> getProcessorList(List<String> processorNames, ProcessorContext context) {
        List<Processor> processorList = Lists.newArrayListWithCapacity(processorNames.size());
        processorNames.forEach(processorName -> {
            try {
                String newProcessorName = processorName;
                if (StringUtils.contains(processorName, StringPool.DOT)) {
                    newProcessorName = StringUtils.replace(processorName, StringPool.DOT, StringPool.SLASH);
                }
                Processor processor = TppObjectFactory.getBean(newProcessorName, Processor.class);
                if(processor!=null) {
                    processorList.add(processor);
                	TppObjectFactory.getBean(newProcessorName, Processor.class);
                  }
            } catch (Throwable t) {
//                ServiceFactory.getSolutionLogger().error(t.getMessage(), t);
            }
        });
        return processorList;
    }

    private   List<String> getConfig(String configKey) {
        String processorStr = TppConfigUtil.getString(configKey, StringUtils.EMPTY);
        if (StringUtils.isEmpty(processorStr)) {
            return Collections.emptyList();
        }
        return StrUtils.strToList(processorStr, StringPool.COLON);
    }

    private Map<String, DataSet<Row>> executeProcessorChain(ProcessorContext processorContext, List<Processor> processorList) {
        Map<String, DataSet<Row>> dataSetMap = Maps.newHashMap();
        for (Processor processor : processorList) {
            try {
                dataSetMap = processor.process(processorContext, dataSetMap);
            } catch (Exception e) {
//                ServiceFactory.getSolutionLogger().error(e.getMessage(), e);
            }
        }
        return dataSetMap;
    }
    
    protected List processResult(ProcessorContext context, DataSet<Row> result) {
        return Objects.nonNull(result) ? DataParseUtils.parseDataSetToBaseDto(result) : Lists.newArrayList();
    }
}
