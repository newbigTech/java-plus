package org.java.plus.dag.core.dataflow.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * ����:
 * <p>
 org.java.plus.dagon.frame.dataflow.core.annotation.DataSetOutput
 *
 * @author jaye
 * @date 2019/2/26
 * <p>
 * config_start:org.java.plus.dagon.frame.dataflow.core.annotation.DataSetOutput||jaye| config_end:
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Outputs {
    Output[] value();
}