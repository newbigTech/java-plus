package org.java.plus.dag.core.base.model;

/**
 * All processor parameter key must implement this interface
 *
 * @author seven.wxy
 * @date 2018/10/19
 */
public interface ParameterConfig {

    /**
     * get parameter name
     *
     * @return
     */
    String getName();

    /**
     * get parameter desc
     *
     * @return
     */
    String getDesc();

    /**
     * get parameter default value
     *
     * @param <T>
     * @return
     */
    <T> T getDefaultValue();

}
