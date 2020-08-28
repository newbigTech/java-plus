package com.dag.engine.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

public class DagEngine {

    private final static ExecutorService executorPool = new ThreadPoolExecutor(50, 500, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1000));
	public static Map<String, Vertex> vertexMap = new HashMap<String, Vertex>();

	public void run(Set set) {
		List<CompletableFuture> completableFutures = Lists.newArrayList();
		set.forEach(id->{ 
			CompletableFuture<Vertex> asyncFuture = CompletableFuture.supplyAsync(() -> vertexMap.get(id).run());
			completableFutures.add(asyncFuture);
		});
		CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()])).join();
	}

}
