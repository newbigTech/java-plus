package org.java.plus.dag.core.base.mvel2.util;

import org.java.plus.dag.core.base.mvel2.integration.VariableResolverFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Mike Brock <cbrock@redhat.com>
 */
public interface StaticStub extends Serializable {
    Object call(Object ctx, Object thisCtx, VariableResolverFactory factory, Object[] parameters)
        throws IllegalAccessException, InvocationTargetException;
}
