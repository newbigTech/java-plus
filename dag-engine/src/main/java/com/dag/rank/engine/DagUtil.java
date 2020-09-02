package com.dag.rank.engine;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DagUtil {
	/**
	   *  拓扑排序检测 是否有环
	   */
//	  public boolean isCircularity(DAG dag ) { 
//		 
//	    Map<String, Integer> inDegreeIntMap = getIntegerMap(dag.getInDegree());
//	    //入度为0的节点
//	    Set sources = computeZeroEdgeVertices(dag.getInDegree());
//	    LinkedList<String> queue = new LinkedList();
//	    queue.addAll(sources);
//	    while (!queue.isEmpty()) {
//	    	String o = queue.removeFirst();
//	      dag.getOutDegree().get(o)
//	          .forEach(so -> {
//	        	  inDegreeIntMap.put(so, inDegreeIntMap.get(so) - 1);
//					if (inDegreeIntMap.get(so) == 0) {
//						queue.add(so);
//					} 
//	          });
//	    }
//	    return dag.getInDegree().values().stream().filter(x -> x.intValue() > 0).count() > 0;
//	  }
	  
		private Map<String, Integer> getIntegerMap(Map<String, Set<String>> inDegree) {
			Set<String> set = inDegree.keySet();
			// 入度表
			return set.stream().collect(Collectors.toMap(k -> k, k -> inDegree.get(k).size()));
		}

		// 入度为0的节点
		private Set computeZeroEdgeVertices(Map<String, Set<String>> inDegree) {
			Set candidates = inDegree.keySet();
			Set roots = new LinkedHashSet(candidates.size());
			for (Iterator it = candidates.iterator(); it.hasNext();) {
				Object candidate = it.next();
				if (inDegree.get(candidate).isEmpty()) {
					roots.add(candidate);
				}
			}
			return roots;
		}
}
