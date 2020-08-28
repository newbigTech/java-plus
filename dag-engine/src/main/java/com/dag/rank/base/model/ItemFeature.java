package com.dag.rank.base.model;

public class ItemFeature {

	private long itemId;
    private int saleQuantity;
    private int tagId;
	private float score;
    
	public long getItemId(){ return itemId; }
	public void setItemId(long itemId){ this.itemId = itemId; }
	public int getSaleQuantity(){ return saleQuantity; }
	public void setSaleQuantity(int saleQuantity){ this.saleQuantity = saleQuantity; }
	public int getTagId(){ return tagId; }
	public void setTagId(int tagId){ this.tagId = tagId; }

    public float getScore(){ return score; }
	public void setScore(float score){ this.score = score; }
}
