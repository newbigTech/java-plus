package com.dag.engine;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class Test {
	static ExecutorService executorService = Executors.newFixedThreadPool(10);

	public static void main(String[] args) {

	    DAG dag = DAG.create();
		Vertex a = new Vertex("a", "111-a");
		Vertex b = new Vertex("b", "222-b");
		Vertex c = new Vertex("c", "333-c");
		Vertex d = new Vertex("d", "444-d");
		Vertex e = new Vertex("e", "555-e");
		Vertex f = new Vertex("f", "666-f");
		Vertex g = new Vertex("g", "777-g");
		Vertex h = new Vertex("h", "888-h");
		Vertex j = new Vertex("j", "999-j");
		dag.addVertex(a);
		dag.addVertex(b);
		dag.addVertex(c);
		dag.addVertex(d);
		dag.addVertex(e);
		dag.addVertex(f);
		dag.addVertex(g);
		dag.addVertex(h);
		dag.addVertex(j);
		dag.addEdge(h, g);
		dag.addEdge(g, b);
		dag.addEdge(a, b);
		dag.addEdge(b, f);
		dag.addEdge(c, d);
		dag.addEdge(d, e);
		dag.addEdge(e, f);
		dag.addEdge(f, j);
		 
	    DAG chain = dag.chain(); 

		System.out.println(dag);
		Map<String, Vertex> vertexMap = dag.getVertexMap();
		dag.execute(col -> {
			Set set = (Set) col;
			System.out.println("Set：" + set);
			List<CompletableFuture> completableFutures = Lists.newArrayList();
			StringBuilder sb = new StringBuilder();
			set.stream().forEach(x -> {
				Vertex vertex = vertexMap.get(x);
				System.out.println("x：" + x.toString());
				CompletableFuture<Vertex> future = CompletableFuture.supplyAsync(() -> vertex.run(), executorService);
				completableFutures.add(future);
				sb.append(" task detached:" + vertex.getName()).append(",");

//				if (x instanceof List) {
//					List<Task> taskList = (List) x;
//					CompletableFuture<Void> future = CompletableFuture.runAsync(() -> taskList.forEach(Task::run));
//					completableFutures.add(future);
//					sb.append(" task chain " + Joiner.on("-")
//							.join(taskList.stream().map(Task::getTaskName).collect(Collectors.toList())));
//				}
			});
			CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
					.join();
			System.out.println("stage 结束 ： " + sb.toString());
			System.out.println("-----------------------------------------------");
		});

		System.out.println("---------end-------------");
	}

}
