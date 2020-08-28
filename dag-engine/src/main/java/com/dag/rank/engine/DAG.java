package com.dag.rank.engine;

import java.util.HashMap;
import java.util.Iterator; 
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

public class DAG {

	private Map<String, Vertex> vertexMap = new HashMap<String, Vertex>();
	private Map<String, Set<String>> inDegree = new HashMap<String, Set<String>>();
	private Map<String, Set<String>> outDegree = new HashMap<String, Set<String>>();

	public DAG() {
	}

	public DAG(Map<String, Set<String>> inDegree, Map<String, Set<String>> outDegree) {
		this.inDegree = inDegree;
		this.outDegree = outDegree;
	}

	public Map<String, Set<String>> getInDegree() {
		return inDegree;
	}

	public Map<String, Set<String>> getOutDegree() {
		return outDegree;
	}

	public Map<String, Vertex> getVertexs() {
		return vertexMap;
	}

	public void addVertex(Vertex vertex) {
		if (!inDegree.containsKey(vertex.getId())) {
			inDegree.put(vertex.getId(), Sets.newLinkedHashSet());
		}
		if (!outDegree.containsKey(vertex.getId())) {
			outDegree.put(vertex.getId(), Sets.newLinkedHashSet());
		}
		vertexMap.put(vertex.getId(), vertex);
	}

	public boolean addEdge(Vertex from_Vertex, Vertex to_Vertex) {
//		if (hasPath(to_Vertex, from_Vertex)) {
//			return false;
//		}
		addVertex(from_Vertex);
		addVertex(to_Vertex);
		inPut(from_Vertex, to_Vertex);
		outPut(from_Vertex, to_Vertex);

		vertexMap.put(from_Vertex.getId(), from_Vertex);
		vertexMap.put(to_Vertex.getId(), to_Vertex);
		return true;
	}

	private boolean hasPath(Object start, Object end) {
		if (start == end) {
			return true;
		}
		Set children = outDegree.get(start);
		for (Iterator it = children.iterator(); it.hasNext();) {
			if (hasPath(it.next(), end)) {
				return true;
			}
		}
		return false;
	}

	public void inPut(Vertex from_Vertex, Vertex to_Vertex) {
		Set<String> values = inDegree.get(to_Vertex.getId());
		if (values == null) {
			values = Sets.newLinkedHashSet();
		}
		values.add(from_Vertex.getId());
		inDegree.put(to_Vertex.getId(), values);
	}

	public void outPut(Vertex from_Vertex, Vertex to_Vertex) {
		Set<String> values = outDegree.get(from_Vertex.getId());
		if (values == null) {
			values = Sets.newLinkedHashSet();
		}
		values.add(to_Vertex.getId());
		outDegree.put(from_Vertex.getId(), values);
	}

	public String toString() {
		return "OutDegree: " + outDegree.toString() + " InDegree: " + inDegree.toString();
	}
}
