package com.dag.rank.base.model;

public class ItemInfo {

	private long itemId; 
	private float score;
	private ItemFeature itemAttribute;
    
	public ItemFeature getItemAttribute(){ return itemAttribute; }
	public void setItemAttribute(ItemFeature itemAttribute){ this.itemAttribute = itemAttribute; }
	public long getItemId(){ return itemId; }
	public void setItemId(long itemId){ this.itemId = itemId; } 

	public float getScore(){ return score; }
	public void setScore(float score){ this.score = score; }
}
