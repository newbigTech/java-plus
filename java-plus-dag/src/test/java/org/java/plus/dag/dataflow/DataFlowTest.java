package org.java.plus.dag.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.java.plus.dag.core.dataflow.DataFlow;
import org.java.plus.dag.core.dataflow.core.DataFlowCore;
import org.java.plus.dag.core.dataflow.core.Operation;
 
 

public class DataFlowTest  {
	
	public static void main(String[] args) {
		testFlow() ;
		testFlowConcurrent() ;
		testEval();

	}
 
    private  static Operation<String> flow(String name, List<String> output, Operation<String>... inputs) {
        return DataFlow.newOp(ctx -> {
            if (null == inputs || inputs.length <= 0) {
                System.out.println(String.format("run[%s]", name));
            } else {
                StringBuilder dep = new StringBuilder();
                for (Operation<String> op : inputs) {
                    dep.append(op.get()).append(",");
                }
                System.out.println(String.format("run[%s], input[%s]", name, dep.toString()));
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            output.add(name);
            return name;
        }, e -> {
            e.printStackTrace();
            return "";
        }, null, inputs).name(name);
    }
 
    public static void testFlow() {
        {
            List<String> output = new ArrayList<>();
            DataFlow df = DataFlowCore.getInstance(null);
            Operation<String> opA = flow("opA", output);
            Operation<String> lastOp = flow("opD", output, flow("opC", output, flow("opB", output, opA)));
            assertEquals("opD", df.run(lastOp));

            assertEquals("opA", output.get(0));
            assertEquals("opB", output.get(1));
            assertEquals("opC", output.get(2));
            assertEquals("opD", output.get(3));
        }
        {
            List<String> output = new ArrayList<>();
            DataFlow df = DataFlowCore.getInstance(null);
            Operation<String> opA = flow("opA", output);
            Operation<String> lastOp = flow("opD", output, flow("opC", output, flow("opB", output, opA)));
            assertEquals("opD", df.eval(lastOp));

            assertEquals("opA", output.get(0));
            assertEquals("opB", output.get(1));
            assertEquals("opC", output.get(2));
            assertEquals("opD", output.get(3));
        }
    }
 
    public static void testFlowConcurrent() {
        DataFlow df = DataFlowCore.getInstance(null);
        List<String> output = new ArrayList<>();
        Operation<String> opA = flow("opA", output);
        Operation<String> opB = flow("opB", output, opA);
        Operation<String> opC = flow("opC", output, opA);
        Operation<String> opD = flow("opD", output, opB, opC);
        Operation<String> opE = flow("opE", output, opD);
        Operation<String> opF = flow("opF", output, opD);
        Operation<String> opG = flow("opG", output, opE, opF);
        assertEquals("opG", df.run(opG));
        assertEquals("opA", output.get(0));
        assertEquals("opB", output.get(1));
        assertEquals("opC", output.get(2));
        assertEquals("opD", output.get(3));
        assertEquals("opE", output.get(4));
        assertEquals("opF", output.get(5));
        assertEquals("opG", output.get(6));
    }
 
    public static void testEval() {
        DataFlow df = DataFlowCore.getInstance(null);
        List<String> output = new ArrayList<>();
        Operation<String> opA = flow("opA", output);
        Operation<String> opB = flow("opB", output, opA);
        Operation<String> opC = flow("opC", output, opA);
        Operation<String> opD = flow("opD", output, opB, opC);
        Operation<String> opE = flow("opE", output, opD);
        Operation<String> opF = flow("opF", output, opD);
        Operation<String> opG = flow("opG", output, opE, opF);
        assertEquals("opG", df.eval(opG));
        assertEquals("opA", output.get(0));
        assertEquals("opB", output.get(1));
        assertEquals("opC", output.get(2));
        assertEquals("opD", output.get(3));
        assertEquals("opE", output.get(4));
        assertEquals("opF", output.get(5));
        assertEquals("opG", output.get(6));
    }
    
    private static void assertEquals(String str1,String str2) {
    	System.out.println(str1.equals(str2));
    }
}
