package org.java.plus.dag.core.engine.executor;

import java.util.concurrent.Future;

/**
 * Dag engine executor
 */
public interface DagExecutor {
    Future execute(Runnable command);
    String getName();
}
