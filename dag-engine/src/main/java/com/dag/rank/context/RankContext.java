package com.dag.rank.context;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dag.rank.base.model.ItemInfo;
import com.dag.rank.engine.DAG;

public class RankContext {

	private List<ItemInfo> itemInfoList; // 多路召回列表
	private ItemInfo[] itemInfos; // 返回列表
	private DAG dag;
	private Set<String> stageSet; // 每阶段并行或串行执行Node的策略 IProcessor

	private Map<String,Object> mapInfo=new HashMap<String,Object>();
	
	public Map<String, Object> getMapInfo() {
		return mapInfo;
	}

	public void setMapInfo(Map<String, Object> mapInfo) {
		this.mapInfo = mapInfo;
	}

	
	public List<ItemInfo> getItemInfoList() {
		return itemInfoList;
	}

	public void setItemInfoList(List<ItemInfo> itemInfoList) {
		this.itemInfoList = itemInfoList;
	}

	public ItemInfo[] getItemInfos() {
		return itemInfos;
	}

	public void setItemInfos(ItemInfo[] itemInfos) {
		this.itemInfos = itemInfos;
	}

	public DAG getDag() {
		return dag;
	}

	public void setDag(DAG dag) {
		this.dag = dag;
	}

	public Set<String> getStageSet() {
		return stageSet;
	}

	public void setStageSet(Set<String> stageSet) {
		this.stageSet = stageSet;
	}

}
