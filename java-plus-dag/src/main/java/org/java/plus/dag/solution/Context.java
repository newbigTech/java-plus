package org.java.plus.dag.solution;

import java.util.HashMap;
import java.util.Map;

public class Context {
	
	private static Map<String,Object> map=new HashMap<String,Object>();

	public void put(String key,Object value) {
		map.put(key, value);
	}
	
	public Object get(String key) {
		return map.get(key);
	}
	
	public Object getRequestParams(String key) {
		return key;
	}
	
	public Boolean containsKey(String key) {
		return map.containsKey(key);
	}
	
	public long getCurrentAppId() {
		return 10000;
	}
	
	public String getCurrentAbId() {
		return "";
	}
	
}
