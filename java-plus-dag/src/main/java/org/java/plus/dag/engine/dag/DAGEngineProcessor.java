package org.java.plus.dag.engine.dag;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.java.plus.dag.core.base.em.NodeType;
import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorConfig;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.ProcessorNode;
import org.java.plus.dag.core.base.model.Row;
import org.java.plus.dag.core.base.utils.TppObjectFactory;
import org.java.plus.dag.core.engine.DagNode;
import org.java.plus.dag.core.engine.DagProcessor;
import org.java.plus.dag.core.engine.DagStruct;
import org.apache.commons.lang3.StringUtils;

/**
 * DAG execute processor
 *
 * @author seven.wxy
 * @date 2019/1/30
 */
public class DAGEngineProcessor extends DagProcessor {
    @Override
    public void doInit(ProcessorConfig conf) {
        super.doInit(conf);
    }

    @Override
    public DataSet<Row> doProcess(ProcessorContext context, DataSet<Row> mainDataSet, Map<String, DataSet<Row>> dataSetMap) {
        return super.doProcess(context, mainDataSet, dataSetMap);
    }

    /**
     * Get dag node depend relation
     * @return key=instanceKey, value=ProcessorNode
     */
    public Map<String, ProcessorNode> getProcessorNodeList(Map<String, ProcessorNode> result) {
        DagStruct<DagNode> dagStruct = engine.getDag();
        parseNode(dagStruct.getEndPoint(), dagStruct, result);
        return result;
    }

    private ProcessorNode parseNode(DagNode curNode, DagStruct<DagNode> dagStruct, Map<String, ProcessorNode> result) {
        ProcessorNode processorNode = new ProcessorNode();
        processorNode.setId(curNode.getFunction().getFunctionName());
        List<String> parents = dagStruct.getDependedKey(curNode.getName()).stream()
            .map(n -> parseNode(dagStruct.getVertex(n), dagStruct, result).getId()).collect(Collectors.toList());
        processorNode.setParentIds(parents);
        setNodeType(processorNode, curNode);
        if (processorNode.getNodeType() == NodeType.DAG) {
            DAGEngineProcessor dagEngine = TppObjectFactory.getBean(processorNode.getId(), DAGEngineProcessor.class);
            dagEngine.getProcessorNodeList(result);
        }
        result.put(processorNode.getId(), processorNode);
        return processorNode;
    }

    private void setNodeType(ProcessorNode processorNode, DagNode node) {
        NodeType result = NodeType.PROCESSOR;
        if (StringUtils.contains(node.getName(), CONDITION_KEY_FLAG)) {
            result = NodeType.CONDITION;
        } else if (StringUtils.contains(node.getFunction().getFunctionName(), this.getClass().getSimpleName())) {
            result = NodeType.DAG;
        }
        processorNode.setNodeType(result);
    }

}
