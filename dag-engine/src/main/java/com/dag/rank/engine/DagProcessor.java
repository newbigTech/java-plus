package com.dag.rank.engine;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture; 
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit; 
import java.util.stream.Collectors;

import com.dag.rank.biz.face.IProcessor;
import com.dag.rank.context.RankContext;
import com.google.common.collect.Lists;

public class DagProcessor {
    private final static ExecutorService executorPool = new ThreadPoolExecutor(50, 500, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1000));
 
	private RankContext context;
	private int maxTimeout = 0;

	public DagProcessor(RankContext context) {
		System.out.println("stageSet:" + context.getStageSet());
		this.context = context;
	}

	public void run() {
		try {
			List<CompletableFuture> completableFutures = Lists.newArrayList();

			List<String> asyncStageSet = this.context.getStageSet().stream()
					.filter(id -> this.context.getDag().getVertexs().get(id).isAsync()).collect(Collectors.toList());
			List<String> noAsyncStageSet = this.context.getStageSet().stream()
					.filter(id -> !this.context.getDag().getVertexs().get(id).isAsync()).collect(Collectors.toList());

			asyncStageSet.forEach(id -> {
				Node vertex = this.context.getDag().getVertexs().get(id);

				CompletableFuture<Void> asyncFuture = processor(vertex);
				completableFutures.add(asyncFuture);
				maxTimeout = vertex.getTimeout() > maxTimeout ? vertex.getTimeout() : maxTimeout;
			});

			noAsyncStageSet.forEach(id -> {
				Node vertex = this.context.getDag().getVertexs().get(id);
				IProcessor processor = vertex.getProcessor();
				processor.doInit(this.context);
				processor.doProcess(this.context);
			});

			maxTimeout = maxTimeout <= 0 ? 200 : maxTimeout;
			CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
					.get(maxTimeout, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private CompletableFuture<Void> processor(Node vertex) {
		CompletableFuture<Void> asyncFuture = CompletableFuture.runAsync(() -> {

			IProcessor processor = vertex.getProcessor();
			this.context.getMapInfo().put("vid", vertex.getId());

			processor.doInit(this.context);
			processor.doProcess(this.context);
//			System.out.println("i am running  my name is " +vertex.getId() +" , "+ vertex.getName() + "  finish ThreadID: " + Thread.currentThread().getId());

		}, executorPool);
		return asyncFuture;
	}
}
