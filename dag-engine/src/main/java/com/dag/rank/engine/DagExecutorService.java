package com.dag.rank.engine;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DagExecutorService {
	   private final static ExecutorService executorPool = new ThreadPoolExecutor(50, 500, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1000));

	   public static ExecutorService getExecutorService() {
		   return executorPool;
	   }
}
