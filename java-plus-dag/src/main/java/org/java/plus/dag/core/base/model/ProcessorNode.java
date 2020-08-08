package org.java.plus.dag.core.base.model;

import java.util.List;

import org.java.plus.dag.core.base.em.NodeType;
import lombok.Data;

@Data
public class ProcessorNode {
    private String id;
    private List<String> parentIds;
    private NodeType nodeType;
    private String condition;
}