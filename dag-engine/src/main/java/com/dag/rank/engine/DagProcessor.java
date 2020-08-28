package com.dag.rank.engine;
 
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.dag.rank.biz.face.IProcessor;
import com.dag.rank.context.RankContext;
import com.google.common.collect.Lists;

public class DagProcessor {

	private RankContext rankContext;

	public DagProcessor(Set set) {
		rankContext.setStageSet(set);
		this.rankContext = rankContext;
	}

	public void run() {
		List<CompletableFuture> completableFutures = Lists.newArrayList();
		this.rankContext.getStageSet().forEach(id -> {
			CompletableFuture<Void> asyncFuture = processor(id);
			completableFutures.add(asyncFuture);
		});
		CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()])).join();
	}

	private CompletableFuture<Void> processor(String id) {
		CompletableFuture<Void> asyncFuture = CompletableFuture.runAsync(() -> {
			Vertex vertex = this.rankContext.getDag().getVertexs().get(id);
			IProcessor processor = vertex.getProcessor();

			processor.doInit(this.rankContext);
			processor.doProcess(this.rankContext);

		}, DagExecutorService.getExecutorService());
		return asyncFuture;
	}
}
