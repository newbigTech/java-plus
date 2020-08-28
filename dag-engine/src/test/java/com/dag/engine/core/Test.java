package com.dag.engine.core;

public class Test {

	public static void main(String[] args) {
		DAG dag = new DAG();
		Vertex a = new Vertex("a", "111-a");
		Vertex b = new Vertex("b", "222-b");
		Vertex c = new Vertex("c", "333-c");
		Vertex d = new Vertex("d", "444-d");
		Vertex e = new Vertex("e", "555-e");
		Vertex f = new Vertex("f", "666-f");
		Vertex g = new Vertex("g", "777-g");
		Vertex h = new Vertex("h", "888-h");
		Vertex j = new Vertex("j", "999-j");
		dag.addEdge(h, g);
		dag.addEdge(g, b);
		dag.addEdge(a, b);
		dag.addEdge(b, f);
		dag.addEdge(c, d);
		dag.addEdge(d, e);
		dag.addEdge(e, f);
		dag.addEdge(f, j);

		System.out.println(dag);
		DagEngine.vertexMap = dag.getVertexs();
		ADGHelper.execute(dag.getInDegree(), dag.getOutDegree());
	}

}
