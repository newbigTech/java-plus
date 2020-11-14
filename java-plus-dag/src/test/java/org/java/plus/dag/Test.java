package org.java.plus.dag;

import java.util.ArrayList;
import java.util.List;

import org.java.plus.dag.core.base.model.ProcessorContext;

public class Test {

	public static void main(String[] args) {
//		RecommendSolution rec=new RecommendSolution();
//		ProcessorContext context=new ProcessorContext();
//		rec.doRecommend(context);
		
		List<String> list=new ArrayList<String>();
		list.add("1");
		list.add("2");
		list.add("3");
		
		Object[] obj=list.stream().filter(x-> "2".equals(x)).toArray();

		System.out.println(obj.length);
		System.out.println(obj[0]);
	}

}
