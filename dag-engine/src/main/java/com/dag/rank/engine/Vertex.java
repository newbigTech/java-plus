package com.dag.rank.engine;

import com.dag.rank.biz.face.IProcessor;

public class Vertex {

	private String id;
	private String name;
	private String processorName;
	private IProcessor processor;

	public Vertex(String id, String name, String processorName) {
		this.id = id;
		this.name = name;
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
			String className = strategyClassName;
			Class<?> classAvailable = ClassLoader.getSystemClassLoader().loadClass(className);
			if (classAvailable != null) {
				T c = (T) Class.forName(className).newInstance();
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

	public String toString() {
		return "id: " + id + " name: " + name;
	}

}
