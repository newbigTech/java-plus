package org.java.plus.dag.core.base.em;

import org.java.plus.dag.core.base.model.AlgInfo.AlgInfoType;
import org.java.plus.dag.core.base.model.AlgInfo.AlgInfoValueType;

/**
 * @author seven.wxy
 * @date 2019/7/16
 */
public interface AlgInfoKeyEnum extends EnumInterface {
    /**
     * name, output to algInfo key
     *
     * @return
     */
    String getName();

    /**
     * algInfo type, ONLINE or DEBUG, DEBUG algInfo only output in debug mode
     *
     * @return
     */
    AlgInfoType getType();

    /**
     * algInfo value type, MULTI type will append multi value with {@link java.util.LinkedHashSet} to algInfo key
     *
     * @return
     */
    AlgInfoValueType getValueType();
}
