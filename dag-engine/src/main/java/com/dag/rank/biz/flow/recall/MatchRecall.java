package com.dag.rank.biz.flow.recall;

import java.util.List;

import com.dag.rank.base.model.ItemInfo;
import com.dag.rank.base.search.ItemSearchService;
import com.dag.rank.base.search.SearchPara;
import com.dag.rank.biz.face.IProcessor;
import com.dag.rank.context.RankContext;

public class MatchRecall implements IProcessor {

	@Override
	public void doInit(RankContext context) {
		 
		System.out.println("threadId: "  + Thread.currentThread().getId());

		System.out.println("doInit:"+context.getMapInfo().get("vid"));
	}

	@Override
	public void doProcess(RankContext context) {
		try {
			System.out.println("-------------MatchRecall doProcess--------------");
			List<ItemInfo> list = 	ItemSearchService.search(new SearchPara());
			Thread.sleep(100);
			context.setItemInfoList(list);
		} catch (Exception e) {
//			logger.error("ItemWeight doWeight is error", e);
		}
		
	}

}
