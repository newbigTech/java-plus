package com.java.plus.leetcode.graph;

 
public class GraphNode {

	int val;
	boolean status;
	GraphNode[] next;

	/**
	 * 图节点遍历
	 * */
	public void traverse(GraphNode node) {
		if (!node.status) {
			for (GraphNode child : node.next) {
				child.status = true;
				traverse(child);
			}
		}

	}

}
