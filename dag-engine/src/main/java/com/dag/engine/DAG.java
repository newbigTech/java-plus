package com.dag.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class DAG {
	
	private LinkedHashSetMultimap outDegree = new LinkedHashSetMultimap();
	private LinkedHashSetMultimap inDegree = new LinkedHashSetMultimap();

	public static DAG create() {
		return new DAG(new LinkedHashSetMultimap(), new LinkedHashSetMultimap());
	}

	public DAG(final LinkedHashSetMultimap outDegree, final LinkedHashSetMultimap inDegree) {
		this.outDegree = outDegree;
		this.inDegree = inDegree;
	}

	private Map<String, Vertex> vertexMap = new HashMap<String, Vertex>();

	public Vertex getVertex(int id) {
		return vertexMap.get(id);
	}

	public Map<String, Vertex> getVertexMap() {
		return vertexMap;
	}

	public void addVertex(Vertex vertex) {
		outDegree.put(vertex.getId(), null);
		inDegree.put(vertex.getId(), null);
		vertexMap.put(vertex.getId(), vertex);
	}

	public boolean addEdge(Vertex origin, Vertex target) {
	    if (hasPath(target, origin)) {
	      return false;
	    } 
		addEdgeWithNoCheck(origin, target);
		return true;
	}
	
	  private boolean hasPath(Object start, Object end) {
		    if (start == end) {
		      return true;
		    }
		    Set children = outDegree.get(start);
		    for (Iterator it = children.iterator(); it.hasNext(); ) {
		      if (hasPath(it.next(), end)) {
		        return true;
		      }
		    }
		    return false;
		  }

	public boolean addEdgeWithNoCheck(Vertex origin, Vertex target) {
		outDegree.put(origin.getId(), target.getId());
		outDegree.put(target.getId(), null);
		inDegree.put(target.getId(), origin.getId());
		inDegree.put(origin.getId(), null);
		return true;
	}

	public void execute(Consumer consumer) {
		Map<Object, AtomicInteger> inDegreeAtomicIntMap = getObjectAtomicIntegerMap();

		System.out.println("execute-AtomicIntegerinDegree：" + inDegreeAtomicIntMap);
		// 入度为0的节点
		Set sources = getSources();
		execute_(sources, inDegreeAtomicIntMap, consumer);
	}

	private Map<Object, AtomicInteger> getObjectAtomicIntegerMap() {
		Set<Object> set = inDegree.keySet();
		// 入度表
		return set.stream().collect(Collectors.toMap(k -> k, k -> new AtomicInteger(this.inDegree.get(k).size())));
	}

	public Set getSources() {
		return computeZeroEdgeVertices(inDegree);
	}

	private Set computeZeroEdgeVertices(LinkedHashSetMultimap map) {
		Set candidates = map.keySet();
		Set roots = new LinkedHashSet(candidates.size());
		for (Iterator it = candidates.iterator(); it.hasNext();) {
			Object candidate = it.next();
			if (map.get(candidate).isEmpty()) {
				roots.add(candidate);
			}
		}

		System.out.println("computeZeroEdgeVertices-roots：" + roots);
		return roots;
	}

	public void execute_(Set set, Map<Object, AtomicInteger> inDegreeAtomicIntMap, Consumer consumer) {
		System.out.println("execute_-set：" + set);
		System.out.println("execute_-inDegreeAtomicIntMap：" + inDegreeAtomicIntMap);
		exec(set, consumer);
		Set nextSet = Sets.newLinkedHashSet();
		set.forEach(o -> {
			outDegree.get(o).forEach(so -> {
				if (inDegreeAtomicIntMap.get(so).decrementAndGet() == 0) {
					nextSet.add(so);
				}
			});
		});
		System.out.println("execute_-inDegreeAtomicIntMap2：" + inDegreeAtomicIntMap);
		System.out.println("execute_-nextSet：" + nextSet);
		if (CollectionUtils.isNotEmpty(nextSet)) {
			execute_(nextSet, inDegreeAtomicIntMap, consumer);
		}
	}

	private void exec(Set set, Consumer consumer) {
		System.out.println("exec-set-start：" + set);
		consumer.accept(set);
		System.out.println("exec-set-end：" + set);
	}

	public String toString() {
		return "OutDegree: " + outDegree.toString() + " InDegree: " + inDegree.toString();
	}

	public DAG chain() {
		Set sources = getSources();
		final LinkedHashSetMultimap outDegreeChain = new LinkedHashSetMultimap();
		final LinkedHashSetMultimap inDegreeChain = new LinkedHashSetMultimap();
		chain_(sources, outDegreeChain, inDegreeChain);
		return new DAG(outDegreeChain, inDegreeChain);
	}

	private void chain_(Set sources, final LinkedHashSetMultimap foutChain, final LinkedHashSetMultimap finChain) {
		sources.forEach(sourceNode -> {

			ArrayList<Object> maxStage = Lists.newArrayList();
			findMaxStage(sourceNode, maxStage);
			if (maxStage.size() > 1) { // 存在需要合并的stage
				addVertex(foutChain, finChain, maxStage);// 添加一个新节点
				Object o = maxStage.get(maxStage.size() - 1); // 最后一个节点
				reChain_(foutChain, finChain, maxStage, o);
			}
			if (maxStage.size() == 1) {
				// 不存在需要合并的stage
				addVertex(foutChain, finChain, sourceNode);// 添加一个新节点
				Set subNodes = outDegree.get(sourceNode);
				addSubNodeage(foutChain, finChain, sourceNode, subNodes);
			}
		});
	}

	/**
	 * 寻找最大的stage
	 */
	public void findMaxStage(Object o, List maxStage) {
		maxStage.add(o);
		Set setOut = outDegree.get(o);
		if (setOut.size() == 1) {
			Object subNode = setOut.iterator().next();
			if (inDegree.get(subNode).size() == 1) {
				findMaxStage(subNode, maxStage);
			}
		}
	}

	private void addVertex(LinkedHashSetMultimap fOut, LinkedHashSetMultimap fIn, Object vertex) {
		fOut.put(vertex, null);
		fIn.put(vertex, null);
	}

	private void reChain_(final LinkedHashSetMultimap foutChain, final LinkedHashSetMultimap finChain,
			ArrayList<Object> maxStage, Object o) {
		Set set = outDegree.get(o); // 最后一个节点的子节点
		Object header = maxStage.get(0); // 第一个stage节点
		// 处理父节点
		Set parentSet = finChain.get(header);
		// 下面操作就是相当于 链表中添加一个节点 A -> B ==> A -> C -> B 先删掉老的A->B 添加新的A->C 在添加新的C->B
		if (CollectionUtils.isNotEmpty(parentSet)) {
			parentSet.forEach(h -> {
				// 删除老的边
				removeEage(foutChain, finChain, h, header);
				// 添加新的表
				addEdge(foutChain, finChain, h, maxStage);
			});
		}
		addSubNodeage(foutChain, finChain, maxStage, set);
	}

	public boolean removeEage(LinkedHashSetMultimap fOut, LinkedHashSetMultimap fIn, Object origin, Object target) {
		fOut.remove(origin, target);
		if (CollectionUtils.isEmpty(fOut.get(origin))) {
			fOut.removeAll(origin);
		}
		fIn.remove(target, origin);
		if (CollectionUtils.isEmpty(fIn.get(target))) {
			fIn.removeAll(target);
		}
		return true;
	}

	private void addSubNodeage(final LinkedHashSetMultimap foutChain, final LinkedHashSetMultimap finChain,
			final Object sourceNode, final Set subNodes) {
		if (CollectionUtils.isNotEmpty(subNodes)) { // 多个出度
			subNodes.forEach(snode -> {
				addEdge(foutChain, finChain, sourceNode, snode);
			});
			chain_(subNodes, foutChain, finChain);
		} else { // 最后一个节点了 把节点添加进去 即可
			addVertex(foutChain, finChain, sourceNode);
		}
	}

	private boolean addEdge(LinkedHashSetMultimap fOut, LinkedHashSetMultimap fIn, Object origin, Object target) {
		fOut.put(origin, target);
		fIn.put(target, origin);
		return true;
	}

}
