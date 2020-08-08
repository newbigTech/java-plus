package org.java.plus.dag;

import org.java.plus.dag.core.base.model.ProcessorContext;

public class Test {

	public static void main(String[] args) {
		RecommendSolution rec=new RecommendSolution();
		ProcessorContext context=new ProcessorContext();
		rec.doRecommend(context);
	}

}
