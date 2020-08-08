package org.java.plus.dag.core.base.em;

import org.java.plus.dag.core.base.model.AlgInfo.AlgInfoType;
import org.java.plus.dag.core.base.model.AlgInfo.AlgInfoValueType;
import lombok.Getter;

/**
 * @author seven.wxy
 * @date 2019/7/16
 */
public enum AlgInfoKeyDebug implements AlgInfoKeyEnum {
    /**
     * play log info
     */
    PLAY_LOG_INFO("playLogInfo")

    /**
     * content publish type
     */
    ,PUBLISH_TYPE("publishType")

    /**
     * reduce score
     */
    ,REDUCE_SCORE("reduceScore")

    /**
     * reduce trigger
     */
    ,REDUCE_TRIGGER("reduceTrig")

    /**
     * scatter flag
     */
    ,SPLIT("split")

    /**
     * other info
     */
    ,OTHER("other")
    ;

    @Getter
    public String name;
    @Getter
    public AlgInfoType type = AlgInfoType.DEBUG;
    @Getter
    public AlgInfoValueType valueType = AlgInfoValueType.SINGLE;

    AlgInfoKeyDebug(String name) {
        this.name = name;
    }

    AlgInfoKeyDebug(String name, AlgInfoValueType valueType) {
        this.name = name;
        this.valueType = valueType;
    }
}
