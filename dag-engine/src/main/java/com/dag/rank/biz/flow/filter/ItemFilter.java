package com.dag.rank.biz.flow.filter;

import java.util.ArrayList;
import java.util.List;

import com.dag.rank.base.model.ItemInfo;
import com.dag.rank.biz.face.IProcessor;
import com.dag.rank.context.RankContext;

public class ItemFilter implements IProcessor {

	@Override
	public void doInit(RankContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void doProcess(RankContext context) {
		List<ItemInfo> list = new ArrayList<ItemInfo>();
		context.setItemInfoList(context.getItemInfoList().subList(0, 5));
	}

}
