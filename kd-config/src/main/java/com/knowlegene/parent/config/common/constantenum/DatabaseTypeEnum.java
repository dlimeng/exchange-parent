package com.knowlegene.parent.config.common.constantenum;

/**
 * 数据类型
 *
 * @Author: limeng
 * @Date: 2019/7/19 16:42
 */
public enum DatabaseTypeEnum {
    /**
     * 各个源
     */
    HIVE(1,"hive"),ORACLE(2,"oracle"),MYSQL(3,"mysql")
    ,HDFS(4,"hdfs"),HIVEDB1(1,"h1");


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

    DatabaseTypeEnum() {
    }

    DatabaseTypeEnum(int value, String name) {
        this.value = value;
        this.name = name;
    }
}
