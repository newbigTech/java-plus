package org.java.plus.dag.core.base.utils.tair;

import java.io.Serializable;

/**
 * @author seven.wxy
 * @date 2018/6/25
 */
public interface ILdbDataMerge {
    Serializable merge(Serializable oldVersionData, Serializable newVersionData);
}
