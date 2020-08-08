package org.java.plus.dag.core.base.model;

import java.util.List;
import java.util.Objects;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * ���ݳ�Id �󶨹�ϵ
 * ���ݳ�ID�� ���Ͷ��ID(publish_id) �� scg_id
 * @author seth.zjw
 * @version V1.0
 * @Title: ContentBinding
 * @date 2018/9/3 ����4:13
 */
public class ContentBinding {
    @JSONField(name = "pool_id")
    private Long poolId;

    @JSONField(name = "app_id")
    private String appId;

    @JSONField(name = "appid")
    private int appid;

    @JSONField(name = "type")
    private ContentTypeEnum type;

    @JSONField(name = "item_ids")
    private List<Long> itemIds;

    public Long getPoolId() {
        return poolId;
    }

    public void setPoolId(Long poolId) {
        this.poolId = poolId;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public int getAppid() {
        return appid;
    }

    public void setAppid(int appid) {
        this.appid = appid;
    }

    public String getType() {
        return Objects.isNull(type) ? null : type.getType();
    }

    public ContentTypeEnum getTypeEnum() {
        return type;
    }

    public void setType(String type) {
        this.type = ContentTypeEnum.valueOf(type);
    }

    public List<Long> getItemIds() {
        return itemIds;
    }

    public void setItemIds(List<Long> itemIds) {
        this.itemIds = itemIds;
    }

    public ContentBinding(Long poolId, String appId, int appid) {
        this.poolId = poolId;
        this.appId = appId;
        this.appid = appid;
    }

    public ContentBinding() {
    }

    @Override
    public String toString() {
        return "PoolBinding{" +
                "poolId=" + poolId +
                ", appId=" + appId +
                ", appid=" + appid +
                ", type=" + type +
                ", itemIds=" + itemIds +
                '}';
    }


}
