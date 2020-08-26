package org.java.plus.dag.util;

public class Test {

	public static void main(String[] args) throws Exception  {
		GenerateDoc gen=new GenerateDoc();
		gen.generateConfigDoc();
		
		try {
			gen.generateConfigDocAll();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
