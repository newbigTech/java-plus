package org.java.plus.dag.util;

import java.util.HashMap;
import java.util.Map;

public class OffHeapCache {
	private static Map<String,Object> map=new HashMap<String,Object>();
	
	public static void put(String key,Object value, Long ttlMs) {
		map.put(key, value);
	}

	public static void put(String key,Object value) {
		map.put(key, value);
	}
	
	public static Object get(String key) {
		return map.get(key);
	}

}
