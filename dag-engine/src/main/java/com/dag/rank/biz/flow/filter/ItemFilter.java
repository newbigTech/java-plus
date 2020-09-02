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
		System.out.println("doInit:"+context.getMapInfo().get("vid"));

		System.out.println("doInit: "  + Thread.currentThread().getId());
	}

	@Override
	public void doProcess(RankContext context) {
		System.out.println("-------------ItemFilter doProcess--------------");
	    try {
			Thread.sleep(100);
		
		List<ItemInfo> list = new ArrayList<ItemInfo>();
		int size=context.getItemInfoList().size();
		context.setItemInfoList(context.getItemInfoList().subList(0, size/2));

		System.out.println("size:"+context.getItemInfoList().size());
	    } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
