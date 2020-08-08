package org.java.plus.dag.core.base.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.java.plus.dag.core.base.constants.ConstantsFrame;
import org.apache.commons.lang3.StringUtils;

/**
 * Only support basic type, Not support Collection type attribute init
 * @author seven.wxy
 * @date 2018/11/2
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ConfigInit {
    String name() default StringUtils.EMPTY;
    String defaultValue() default ConstantsFrame.DEFAULT_CONFIG_VALUE;
    String desc();
    String nameCn() default StringUtils.EMPTY;
    long min() default Long.MIN_VALUE;
    long max() default Long.MAX_VALUE;
    boolean required() default false;
}
