package org.java.plus.dag.core.base.mvel2.util;

import org.java.plus.dag.core.base.mvel2.integration.VariableResolver;
import org.java.plus.dag.core.base.mvel2.integration.VariableResolverFactory;
import org.java.plus.dag.core.base.mvel2.integration.impl.IndexVariableResolver;
import org.java.plus.dag.core.base.mvel2.integration.impl
    .IndexedVariableResolverFactory;
import org.java.plus.dag.core.base.mvel2.integration.impl.SimpleValueResolver;

/**
 * @author Mike Brock .
 */
public class SimpleVariableSpaceModel extends VariableSpaceModel {
    public SimpleVariableSpaceModel(String[] varNames) {
        this.allVars = varNames;
    }

    public VariableResolverFactory createFactory(Object[] vals) {
        VariableResolver[] resolvers = new VariableResolver[allVars.length];
        for (int i = 0; i < resolvers.length; i++) {
            if (i >= vals.length) {
                resolvers[i] = new SimpleValueResolver(null);
            } else {
                resolvers[i] = new IndexVariableResolver(i, vals);
            }
        }

        return new IndexedVariableResolverFactory(allVars, resolvers);
    }
}
