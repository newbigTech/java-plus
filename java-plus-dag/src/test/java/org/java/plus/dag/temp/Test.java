package org.java.plus.dag.temp;

import java.util.Objects;

import org.java.plus.dag.core.base.utils.ThreadLocalUtils;

import com.alibaba.fastjson.JSONObject;

public class Test {

	public static void main(String[] args) {
		  JSONObject currentConfig  = ThreadLocalUtils.getMockTppConfig();
		  System.out.println("test");

	}

}
