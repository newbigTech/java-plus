package com.dag.engine.core;

import com.dag.rank.biz.face.IProcessor;

import lombok.Data;

@Data
public class Vertex {
	
	private String id;
	private String name; 
	
	public Vertex(String id, String name) {
		this.id=id;
		this.name=name;
	} 
	 
	public Vertex run() {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("i am running  my name is " +id+" , "+ name + "  finish ThreadID: " + Thread.currentThread().getId());
		return new Vertex( id,  name) ;
	}
	
	public String toString() {
		return "id: " + id + " name: " + name;
	}

}
