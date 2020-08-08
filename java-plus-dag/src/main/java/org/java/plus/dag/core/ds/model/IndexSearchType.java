package org.java.plus.dag.core.ds.model;

public enum IndexSearchType {

    OR("or"),
    AND("and"),
    ANDNOT("andnot");

    private String name;

    private IndexSearchType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
