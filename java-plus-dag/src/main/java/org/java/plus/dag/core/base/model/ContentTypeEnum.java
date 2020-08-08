package org.java.plus.dag.core.base.model;

public enum ContentTypeEnum {
    //SCGID
    SCG("SCG"),
    //Ͷ��id
    PUBLISH("PUBLISH");

    private final String type;

    ContentTypeEnum(String type) {    //enum��Ĺ��캯��
        this.type = type;
    }

    public String getType() {
        return type;
    }

}
