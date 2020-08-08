
package org.java.plus.dag.core.ds.factory;

import java.util.List;
import java.util.Map;

/**
 * IGraphDataSourceConfig interface
 */
public interface IGraphDataSourceConfig {

    IGraphDataSourceConfigPojo getSingleConfig();

    Map<String, IGraphDataSourceConfigPojo> getAllConfig();

    List<String> getRequestTableList();

    String getRequestTableString();

    boolean isAsync();
}
