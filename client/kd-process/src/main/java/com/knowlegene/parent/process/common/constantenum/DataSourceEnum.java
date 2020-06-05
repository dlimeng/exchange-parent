package com.knowlegene.parent.process.common.constantenum;

/**
 * 不同源的标志
 * @Author: limeng
 * @Date: 2019/7/23 14:56
 */
public enum DataSourceEnum {
    /**
     * 各个源
     */
    HIVE(1,"hive"),ORACLE(2,"oracle"),MYSQL(3,"mysql")
    ,HDFS(4,"hdfs"),NEOURL(5,"url"),NEOUSERNAME(6,"username"),NEOPASSWORD(7,"password");

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

    DataSourceEnum() {
    }

    DataSourceEnum(int value, String name) {
        this.value = value;
        this.name = name;
    }
}
