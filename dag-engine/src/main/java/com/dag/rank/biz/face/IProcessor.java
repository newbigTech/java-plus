package com.dag.rank.biz.face;

import com.dag.rank.context.RankContext;

public interface IProcessor {
	
	public void doInit(RankContext context);

	public void doProcess(RankContext context);
}
