//package com.dag.engine.core;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import org.apache.commons.collections4.CollectionUtils;
//
//import com.dag.engine.LinkedHashSetMultimap;
//import com.google.common.collect.Lists;
//
//public class DagChain {
//	public Map<String, Set<String>> inDegree;
//	public Map<String, Set<String>> outDegree;
//	private Map<String, Vertex> vertexMap ;
//
//	public Map<Set<String>, Set<String>> inChainDegree;
//	public Map<Set<String>, Set<String>> outChainDegree;
//
//	public DagChain(Map<String, Set<String>> inDegree, Map<String, Set<String>> outDegree, Map<String, Vertex> vertexMap) {
//		this.inDegree = inDegree;
//		this.outDegree = outDegree;
//		this.vertexMap = vertexMap;
//	}	
//
//	public DagChain(Map<Set<String>, Set<String>>inChainDegree,Map<Set<String>, Set<String>> outChainDegree ) {
//		this.inChainDegree = inChainDegree;
//		this.outChainDegree = outChainDegree; 
//	}
//
//	public DagChain chain() {
//		Set sources = ADGHelper.computeZeroEdgeVertices(inDegree);
//		Map<Set<String>, Set<String>> outDegreeChain = new HashMap<Set<String>, Set<String>>();
//		Map<Set<String>, Set<String>> inDegreeChain = new HashMap<Set<String>, Set<String>>();
//		chain_(sources, outDegreeChain, inDegreeChain);
//		return new DagChain(outDegreeChain, inDegreeChain);
//	}
//
//	private void chain_(Set sources, Map<Set<String>, Set<String>> foutChain, Map<Set<String>, Set<String>> finChain) {
//		sources.forEach(sourceNode -> {
//
//			ArrayList<String> maxStage = Lists.newArrayList();
//			findMaxStage(sourceNode, maxStage);
//			if (maxStage.size() > 1) { // 存在需要合并的stage
//				addVertex(foutChain, finChain, maxStage);// 添加一个新节点
//				String o = maxStage.get(maxStage.size() - 1); // 最后一个节点
//				reChain_(foutChain, finChain, maxStage, o);
//			}
//			if (maxStage.size() == 1) {
//				// 不存在需要合并的stage
//				addVertex(foutChain, finChain, sourceNode);// 添加一个新节点
//				Set subNodes = outDegree.get(sourceNode);
//				addSubNodeage(foutChain, finChain, sourceNode, subNodes);
//			}
//		});
//	}
//
//	/**
//	 * 寻找最大的stage
//	 */
//	public void findMaxStage(Object o, List maxStage) {
//		maxStage.add(o);
//		Set setOut = outDegree.get(o);
//		if (setOut.size() == 1) {
//			Object subNode = setOut.iterator().next();
//			if (inDegree.get(subNode).size() == 1) {
//				findMaxStage(subNode, maxStage);
//			}
//		}
//	}
//
//	private void addSubNodeage(Map<Set<String>, Set<String>> foutChain, Map<Set<String>, Set<String>> finChain, String sourceNode,
//			Set subNodes) {
//		if (CollectionUtils.isNotEmpty(subNodes)) { // 多个出度
//			subNodes.forEach(snode -> {
//				addEdge(foutChain, finChain, sourceNode, snode);
//			});
//			chain_(subNodes, foutChain, finChain);
//		} else { // 最后一个节点了 把节点添加进去 即可
//			addVertex(foutChain, finChain, sourceNode);
//		}
//	}
//
//	private boolean addEdge(Map<Set<String>, Set<String>>  fOut, Map<Set<String>, Set<String>>  fIn, Set<String> origin, Set<String> target) {
//		fOut.put(origin, target);
//		fIn.put(target, origin);
//		return true;
//	}
//
//	private void addVertex( Map<Set<String>, Set<String>> fOut,  Map<Set<String>, Set<String>> fIn, Set<String> vertexIds) {
//		fOut.put(vertexIds, null);
//		fIn.put(vertexIds, null);
//	}
//}
