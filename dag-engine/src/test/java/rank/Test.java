package rank;

import com.dag.rank.context.RankContext;
import com.dag.rank.engine.DAG;
import com.dag.rank.engine.DagEngine;
import com.dag.rank.engine.Node;

public class Test {

	public static void main(String[] args) {
		DAG dag = new DAG();
		String processorName = "com.dag.rank.biz.flow.recall.MatchRecall";
		String processorName2 = "com.dag.rank.biz.flow.filter.ItemFilter";

		boolean async1 = true;
		boolean async2 = true;
		int timeout = 10;
		int timeout2 = 200;

		Node a = new Node("a", "111-a", async1, timeout, processorName);
		Node b = new Node("b", "222-b", async1, timeout, processorName);
		Node c = new Node("c", "333-c", async1, timeout, processorName);
		Node d = new Node("d", "444-d", async1, timeout, processorName);
		Node e = new Node("e", "555-e", async2, timeout, processorName2);
		Node f = new Node("f", "666-f", async2, timeout2, processorName2);
		Node g = new Node("g", "777-g", async1, timeout, processorName);
		Node h = new Node("h", "888-h", async1, timeout, processorName);
		Node j = new Node("j", "999-j", async1, timeout2, processorName2);
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
		try {
			rankContext.setDag(dag);
			DagEngine dagEngine = new DagEngine(rankContext);
			dagEngine.execute();
		} catch ( Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("-------------Test-------------");
		System.out.println(rankContext.getItemInfoList().size());
	}

}
