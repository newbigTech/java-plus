package org.java.plus.dag.core.ds.parser;

import org.java.plus.dag.core.base.model.DataSet;
import org.java.plus.dag.core.base.model.ProcessorContext;
import org.java.plus.dag.core.base.model.Row;

@FunctionalInterface
public interface IGraphRequestHandler {
    DataSet<Row> retrieveProcess(ProcessorContext context, IGraphTypeHandler typeHandler);
}
