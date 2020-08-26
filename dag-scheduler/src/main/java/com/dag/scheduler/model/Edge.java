package com.dag.scheduler.model;

import java.util.UUID;

import lombok.Data;

/**
 * 边
 */
@Data
public class Edge {

	private String id = UUID.randomUUID().toString();
	private String fromV;
	private String toV;
	
    public Edge (String fromV, String toV) { 
        this.fromV = fromV;
        this.toV = toV;
        this.id = fromV+"->"+toV;
    }

//	public Edge(String fromV, String toV) {
//		this.fromV = fromV;
//		this.toV = toV;
//	}
//
//	public String getFromV() {
//		return fromV;
//	}
//
//	public void setFromV(String fromV) {
//		this.fromV = fromV;
//	}
//
//	public String getToV() {
//		return toV;
//	}

	public void setToV(String toV) {
		this.toV = toV;
	}
}
