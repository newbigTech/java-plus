package com.dag.rank.engine;

import com.dag.rank.biz.face.IProcessor;

public class Node {

	private String id;
	private String name;
	private String processorName;
	private IProcessor processor;  
	private boolean async;
	private int timeout;//该processor执行超时时间，单位毫秒

	public Node(String id, String name,boolean async,int timeout, String processorName) {
		this.id = id;
		this.name = name;
		this.async = async;
		this.timeout=timeout;
		this.processorName = processorName;
		setProcessorName(this.processorName);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getProcessorName() {
		return processorName;
	}

	private void setProcessorName(String processorName) {
		this.processorName = processorName;
		try {
			processor = reflectClass(processorName, IProcessor.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	private <T> T reflectClass(String strategyClassName, Class<T> clazz) {
		try { 
			Class<?> classAvailable = ClassLoader.getSystemClassLoader().loadClass(strategyClassName);
			if (classAvailable != null) {
				T c = (T) Class.forName(strategyClassName).newInstance();
				return c;
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return null;
	}

	public IProcessor getProcessor() {
		return processor;
	}
	 
	public boolean isAsync() {
		return async;
	}

	public void setAsync(boolean async) {
		this.async = async;
	}
	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}
	public String toString() {
		return "id: " + id + " name: " + name;
	}

}
