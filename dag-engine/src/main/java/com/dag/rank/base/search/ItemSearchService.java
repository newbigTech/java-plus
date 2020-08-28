package com.dag.rank.base.search;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dag.rank.base.model.ItemInfo;
import com.google.common.collect.Lists;

/**
 * 商品搜索召回
 */
public class ItemSearchService {
    private final static ExecutorService executorPool = new ThreadPoolExecutor(50, 500, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1000));

	private static Logger logger = LoggerFactory.getLogger(ItemSearchService.class);

	public static List<ItemInfo> search(SearchPara para){
		List<ItemInfo> list = Lists.newArrayList();
		ItemInfo itemInfo = new ItemInfo();
		for (int i = para.getCount(); i <para.getCount()+ 10; i++) {
			itemInfo = new ItemInfo();
			itemInfo.setItemId(i);
			// itemInfo.setSaleQuantity(i * 5);
			// itemInfo.setTagId(i % 3);
			list.add(itemInfo);
		}
		return list;
	}

	public static List<ItemInfo> search(){ 
		return search(null);
	}

	/**
	 * 异步发起召回请求
	 * */
	public static CompletableFuture<List<ItemInfo>> asyncSearch(SearchPara para){
		try {
			CompletableFuture<List<ItemInfo>> asyncFuture = CompletableFuture.supplyAsync(() -> search(para));
			return asyncFuture;
		} catch (Exception e) {
			logger.error("ItemSearchService asyncSearch is error", e);
		}
		return null;
	}

	public static CompletableFuture<List<ItemInfo>> asyncSearch1(SearchPara para){
		try {
			CompletableFuture<List<ItemInfo>> asyncFuture = CompletableFuture.supplyAsync(() -> search(para),executorPool);
			return asyncFuture;
		} catch (Exception e) {
			logger.error("ItemSearchService asyncSearch is error", e);
		}
		return null;
	}
}
