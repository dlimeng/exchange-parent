package com.knowlegene.parent.process.common.constantenum;

/**
 * @Author: limeng
 * @Date: 2019/9/29 15:56
 */
public enum Neo4jEnum {

    RELATE(1,"relate"),SAVE(2,"save"),UPDATE(3,"update"),DELETE(4,"delete"),
    START_ID(5,"start_id"),END_ID(6,"end_id"),NODE_ID(7,"node_id"),NODE(8,"node");
    /**
     * 类型标记
     */
    private int value;

    /**
     * 类型名称
     */
    private String name;

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    Neo4jEnum() {
    }

    Neo4jEnum(int value, String name) {
        this.value = value;
        this.name = name;
    }
}
