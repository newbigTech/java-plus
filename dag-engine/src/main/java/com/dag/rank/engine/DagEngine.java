package com.dag.rank.engine;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

import com.dag.rank.context.RankContext;
import com.google.common.collect.Sets;

public class DagEngine {

	private RankContext context;

	public DagEngine(RankContext context) {
		this.context = context;
	}

	public void execute() {
		DAG dag = context.getDag();
		Map<String, Integer> inDegreeIntMap = getIntegerMap(dag.getInDegree());
		// 入度为0的节点
		Set sources = computeZeroEdgeVertices(dag.getInDegree());
		execute_(sources, inDegreeIntMap, dag.getOutDegree());
	}

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

	private void execute_(Set set, Map<String, Integer> inDegreeIntMap, Map<String, Set<String>> outDegree) {
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

	private void exec(Set stageSet) {
		context.setStageSet(stageSet);
		DagProcessor dagProcessor = new DagProcessor(context);
		dagProcessor.run();
	}
}
