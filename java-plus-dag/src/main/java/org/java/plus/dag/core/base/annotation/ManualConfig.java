package org.java.plus.dag.core.base.annotation;

import org.java.plus.dag.core.base.model.ManualType;
import org.apache.commons.lang3.StringUtils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Only support basic type, Not support Collection type attribute init
 *
 * @author seven.wxy
 * @date 2018/11/2
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ManualConfig {
    ManualType type();

    String key() default StringUtils.EMPTY;

    String name();
}
