package org.java.plus.dag;

import org.apache.logging.log4j.core.util.Assert;
import org.java.plus.dag.core.base.proc.BaseProcessor;
import org.java.plus.dag.core.engine.DagStruct;

public class DAGTest {

	public static void main(String[] args) {
		testInit();
	}

	public static void testInit() {

		DagStruct dag = new DagStruct();
	     // 测试正常逻辑
        dag.addVertex("a", new BaseProcessor());
        dag.addVertex("b", new BaseProcessor());
        dag.addVertex("c", new BaseProcessor());
        dag.addVertex("d", new BaseProcessor());
        dag.addEdge("a", "b");
        dag.addEdge("b", "c");
        dag.addEdge("c", "d");

		System.out.println(dag.init());
		
		 // 测试有环
        dag = new DagStruct();
        dag.addVertex("a", new BaseProcessor());
        dag.addVertex("b", new BaseProcessor());
        dag.addVertex("c", new BaseProcessor());
        dag.addVertex("d", new BaseProcessor());
        dag.addEdge("a", "b");
        dag.addEdge("b", "c");
        dag.addEdge("c", "d");
        dag.addEdge("d", "a");
        System.out.println(dag.init());
        
        dag = new DagStruct();
        dag.addVertex("a", new BaseProcessor());
        dag.addEdge("a", "a");
        System.out.println(dag.init());

        // 测试多个终点
        dag = new DagStruct();
        dag.addVertex("a", new BaseProcessor());
        dag.addVertex("b", new BaseProcessor());
        dag.addVertex("c", new BaseProcessor());
        dag.addVertex("d", new BaseProcessor());
        dag.addEdge("a", "b");
        dag.addEdge("b", "c");
        dag.addEdge("b", "d");
        System.out.println(dag.init());

        // 测试异常输入
        dag = new DagStruct();
        System.out.println(dag.init());

	}
}
