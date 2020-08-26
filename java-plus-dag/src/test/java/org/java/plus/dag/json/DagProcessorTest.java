package org.java.plus.dag.json;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.java.plus.dag.RecommendSolution;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.proc.Processor;
import org.java.plus.dag.core.base.utils.ThreadLocalUtils;
import org.java.plus.dag.core.base.utils.TppObjectFactory;
import org.java.plus.dag.engine.dag.DAGEngineProcessor;

import com.alibaba.dubbo.common.json.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
 

public class DagProcessorTest {

	public static void main(String[] args) {
		init();
		String configKey="content"; 
//	    DAGEngineProcessor dagEngine = TppObjectFactory.getBean(configKey, DAGEngineProcessor.class);
	   
		ProcessorContext context=new ProcessorContext();
		context.setMockTppConfig(mockTppConfig); 
//		context.setTppContext(tppContext);
		ThreadLocalUtils.setThreadLocalData(context);
		System.out.println(ThreadLocalUtils.getMockTppConfig());
		
		RecommendSolution rec=new RecommendSolution();
		List<Processor> list= rec.initProcessors(context);
		for(Processor processor : list) {
			System.out.println(processor.getName());
		}
		System.out.println(list.size());
	}
	
	private static JSONObject mockTppConfig;
	
	private static void init() {
		String jsonStr="";
		try {
			jsonStr = FileUtils.readFileToString(new File("C:\\Users\\lejianjun\\git\\java-plus\\java-plus-dag\\src\\main\\resources\\data\\tpp_config\\111.json"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		JSONArray jSONArray=JSONArray.parseArray(jsonStr);
		JSONObject jsonObject = jSONArray.getJSONObject(11);
		mockTppConfig=jsonObject;
		System.out.println("------------mockTppConfig----------------");
		System.out.println(JSONObject.toJSONString(mockTppConfig));
//		ThreadLocalUtils.setMockTppConfig(mockTppConfig);
	}

}
