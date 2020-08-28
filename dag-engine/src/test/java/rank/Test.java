package rank;

import com.dag.rank.context.RankContext;
import com.dag.rank.engine.DAG;
import com.dag.rank.engine.DagEngine;
import com.dag.rank.engine.Vertex;

public class Test {

	public static void main(String[] args) {
		DAG dag = new DAG();
		String processorName = "com.dag.rank.biz.flow.recall.MatchRecall";
		String processorName2 = "com.dag.rank.biz.flow.filter.ItemFilter";
	
		Vertex a = new Vertex("a", "111-a", processorName);
		Vertex b = new Vertex("b", "222-b", processorName);
		Vertex c = new Vertex("c", "333-c", processorName);
		Vertex d = new Vertex("d", "444-d", processorName);
		Vertex e = new Vertex("e", "555-e", processorName);
		Vertex f = new Vertex("f", "666-f", processorName);
		Vertex g = new Vertex("g", "777-g", processorName);
		Vertex h = new Vertex("h", "888-h", processorName);
		Vertex j = new Vertex("j", "999-j", processorName2);
		dag.addEdge(h, g);
		dag.addEdge(g, b);
		dag.addEdge(a, b);
		dag.addEdge(b, f);
		dag.addEdge(c, d);
		dag.addEdge(d, e);
		dag.addEdge(e, f);
		dag.addEdge(f, j);

		System.out.println(dag);

		RankContext rankContext = new RankContext();
		rankContext.setDag(dag);
		DagEngine.setRankContext(rankContext);
		DagEngine.execute(dag.getInDegree(), dag.getOutDegree());
		

		System.out.println(rankContext.getItemInfoList().size());
	}

}
