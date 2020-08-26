package com.dag.scheduler.processor;

import com.dag.scheduler.config.BaseContext;

public interface IProcessor {
	
	public void init(BaseContext context);
	
	public void doProcess(BaseContext context);
	
	public void end(BaseContext context);
	
}
