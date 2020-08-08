package org.java.plus.dag.core.base.em;

import org.java.plus.dag.core.base.utils.EnumUtil;

/**
 * All Enum class use in Row or AlgInfo must register in this class
 * @author seven.wxy
 * @date 2019/7/17
 */
public class RegisterEnumClass {
    public static void registerEnum() {
        EnumUtil.addClass(AlgInfoKey.class);
        EnumUtil.addClass(AlgInfoKeyDebug.class);
        EnumUtil.addClass(AllFieldName.class);
        EnumUtil.addClass(NodeType.class);
        EnumUtil.addClass(RecallType.class);
    }
}
