package org.java.plus.dag.core.ds.parser;

import org.java.plus.dag.core.ds.model.IGraphRequest;
import org.java.plus.dag.core.ds.factory.IGraphDataSourceConfig;

public interface IGraphTypeHandler {

    IGraphRequest getIGraphRequest(IGraphDataSourceConfig iGraphDataSourceConfig);

}