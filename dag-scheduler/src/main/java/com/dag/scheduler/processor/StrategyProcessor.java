package com.dag.scheduler.processor;

import com.dag.scheduler.config.BaseContext;

public class StrategyProcessor implements IProcessor{

	@Override
	public void init(BaseContext context) {
		 System.out.println("init-"+context.getName());
		
	}

	@Override
	public void doProcess(BaseContext context) {
		 System.out.println("doProcess-"+context.getName()); 
	}

	@Override
	public void end(BaseContext context) {
		 System.out.println("end-"+context.getName()); 
		
	}

}
