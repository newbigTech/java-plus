package org.java.plus.dag.core.base.mvel2.util;

import org.java.plus.dag.core.base.mvel2.integration.VariableResolverFactory;

public interface CallableProxy {
    Object call(Object ctx, Object thisCtx, VariableResolverFactory factory, Object[] parameters);
}
