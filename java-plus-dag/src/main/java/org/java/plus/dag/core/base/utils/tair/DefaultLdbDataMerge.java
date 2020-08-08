package org.java.plus.dag.core.base.utils.tair;

import java.io.Serializable;

public class DefaultLdbDataMerge implements ILdbDataMerge {
    @Override
    public Serializable merge(Serializable oldVersionData, Serializable newVersionData) {
        return newVersionData;
    }
}
