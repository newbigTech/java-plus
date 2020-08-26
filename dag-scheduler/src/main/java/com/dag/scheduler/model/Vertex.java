package com.dag.scheduler.model;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.dag.scheduler.processor.IProcessor;

import lombok.Data;

/**
 * 顶点信息
 */
@Data
public class Vertex {

	private String id = UUID.randomUUID().toString();
	private IProcessor processor;
	private boolean async;
	private int timeout; // timeout milliseconds 

    private Map<String, Edge> inDegree  = new HashMap<>();
    private Map<String, Edge> outDegree = new HashMap<>();
    private String name;
    
    public Vertex(String name) {
    	this.name= name;
    	this.id= name;
    }
    
    
    public void addInDegree(Edge e)  {
//        checkDuplicatedEdge(e, inDegree);
        inDegree.put(e.getId(), e);
    }
    public void addOutDegree(Edge e) {
//        checkDuplicatedEdge(e, outDegree);
        outDegree.put(e.getId(), e);
    }
}
