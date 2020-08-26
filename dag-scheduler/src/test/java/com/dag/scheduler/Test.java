package com.dag.scheduler;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture; 

import com.alibaba.fastjson.JSON;
import com.dag.scheduler.build.DagEngine;
import com.dag.scheduler.model.Vertex;

public class Test {

	public static void main(String[] args) { 
		Vertex A=new Vertex("A");
		Vertex B=new Vertex("B");
		Vertex C=new Vertex("C");
		Vertex D=new Vertex("D");
		Vertex E=new Vertex("E");
		Vertex F=new Vertex("F");
		Vertex G=new Vertex("G");
		Vertex H=new Vertex("H");
		Vertex I=new Vertex("I");
		Vertex J=new Vertex("J");
		Vertex K=new Vertex("K");
		DagEngine.putEdge(A,B);  
		DagEngine.putEdge(A,C);  
		DagEngine.putEdge(A,D);  
		DagEngine.putEdge(B, E);  
		DagEngine.putEdge(E, F); 
		DagEngine.putEdge(F, H); 
		DagEngine.putEdge(H, J); 
		DagEngine.putEdge(J, K); 
		DagEngine.putEdge(C, E); 
		DagEngine.putEdge(C, H); 
		DagEngine.putEdge(H, J); 
		DagEngine.putEdge(J, K);  
		DagEngine.putEdge(D, G);  
		DagEngine.putEdge(I, J);  
		
		 Collection<Vertex> vertices =DagEngine.getVertices();
		 System.out.println(	vertices.size());
		 System.out.println(JSON.toJSONString(vertices));
		 DagEngine.getVertices().parallelStream().forEach( v -> { 
			 
	        });
		 
	 }

}
