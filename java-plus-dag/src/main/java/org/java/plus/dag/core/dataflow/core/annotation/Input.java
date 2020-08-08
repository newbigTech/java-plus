package org.java.plus.dag.core.dataflow.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * √Ë ˆ:
 * <p>
 * org.java.plus.dag.frame.dataflow.core.annotation.DataSetInput
 *
 * @author jaye
 * @date 2019/2/26
 * <p>
 * config_start: |org.java.plus.dag.frame.dataflow.core.annotation.DataSetInput||jaye| config_end:
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Inputs.class)
public @interface Input {
    int position();
    String rowType() default "";
    String objType() default "";
    String desc() default "";
}