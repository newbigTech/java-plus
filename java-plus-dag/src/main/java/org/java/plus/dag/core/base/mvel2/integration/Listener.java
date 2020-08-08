package org.java.plus.dag.core.base.mvel2.integration;

public interface Listener {
    public void onEvent(Object context, String contextName, VariableResolverFactory variableFactory, Object value);
}
