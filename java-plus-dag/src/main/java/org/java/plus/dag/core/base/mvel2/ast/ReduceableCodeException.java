package org.java.plus.dag.core.base.mvel2.ast;

public class ReduceableCodeException extends RuntimeException {
    private Object literal;

    public Object getLiteral() {
        return literal;
    }

    public ReduceableCodeException(Object literal) {
        this.literal = literal;
    }
}
