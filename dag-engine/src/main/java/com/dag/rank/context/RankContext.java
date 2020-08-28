package com.dag.rank.context;

import java.util.List;
import java.util.Map;
import java.util.Set;
 
import com.dag.rank.base.model.ItemInfo;
import com.dag.rank.engine.DAG;
import com.dag.rank.engine.Vertex; 

public class RankContext {
	
	private List<ItemInfo> itemInfoList;
	private ItemInfo[] itemInfos;
	private DAG dag;  
	private Set<String> stageSet;
	
	public Set<String> getStageSet() {
		return stageSet;
	}
	public void setStageSet(Set<String> stageSet) {
		this.stageSet = stageSet;
	}
	public DAG getDag() {
		return dag;
	}
	public void setDag(DAG dag) {
		this.dag = dag;
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
}
