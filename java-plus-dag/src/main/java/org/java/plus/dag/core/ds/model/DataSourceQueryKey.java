package org.java.plus.dag.core.ds.model;

import org.java.plus.dag.taobao.KeyList;
import org.java.plus.dag.taobao.KeyValuePack;

//import com.taobao.igraph.client.model.KeyList;
//import com.taobao.tair.etc.KeyValuePack;
import lombok.Data;

/**
 * @author seth.zjw
 * @version V1.0
 * @Title: DataSourceQueryKey
 * @Package org.java.plus.dag.frame.base.model
 * @date 2018/11/8 ����3:20
 */
@Data
public class DataSourceQueryKey {

    /**
     * use for iGraph
     */
    private KeyList keyList;
    /**
     * use for Tair
     */
    private KeyValuePack keyValuePack;

    public DataSourceQueryKey() {
    }

    public DataSourceQueryKey(KeyList keyList, KeyValuePack keyValuePack) {
        this.keyList = keyList;
        this.keyValuePack = keyValuePack;
    }

    public DataSourceQueryKey(KeyList keyList) {
        this(keyList, null);
    }

    public DataSourceQueryKey(KeyValuePack keyValuePack) {
        this(null, keyValuePack);
    }
}
