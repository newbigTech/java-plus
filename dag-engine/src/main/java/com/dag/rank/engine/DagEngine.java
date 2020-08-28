package com.dag.rank.engine;
 
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set; 
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils; 
import com.google.common.collect.Sets;

public class DagEngine {

	public static void execute(Map<String, Set<String>> inDegree, Map<String, Set<String>> outDegree) {
		Map<String, Integer> inDegreeIntMap = getIntegerMap(inDegree);
		// 入度为0的节点
		Set sources = computeZeroEdgeVertices(inDegree);
		execute_(sources, inDegreeIntMap, outDegree);
	}

	private static Map<String, Integer> getIntegerMap(Map<String, Set<String>> inDegree) {
		Set<String> set = inDegree.keySet();
		// 入度表
		return set.stream().collect(Collectors.toMap(k -> k, k -> inDegree.get(k).size()));
	}

	// 入度为0的节点
	public static Set computeZeroEdgeVertices(Map<String, Set<String>> inDegree) {
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

	private static void execute_(Set set, Map<String, Integer> inDegreeIntMap, Map<String, Set<String>> outDegree) {
		exec(set);
		Set nextSet = Sets.newLinkedHashSet();
		set.forEach(o -> {
			outDegree.get(o).forEach(so -> {
				inDegreeIntMap.put(so, inDegreeIntMap.get(so) - 1);
				if (inDegreeIntMap.get(so) == 0) {
					nextSet.add(so);
				}
			});
		});
		if (CollectionUtils.isNotEmpty(nextSet)) {
			execute_(nextSet, inDegreeIntMap, outDegree);
		}
	}

	private static void exec(Set set) { 
		DagProcessor dagProcessor = new DagProcessor(set);
		dagProcessor.run();
	} 
}
