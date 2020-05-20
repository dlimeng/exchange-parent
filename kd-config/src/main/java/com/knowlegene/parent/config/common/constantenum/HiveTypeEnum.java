package com.knowlegene.parent.config.common.constantenum;

/**
 * @Author: limeng
 * @Date: 2019/7/23 23:51
 */
public enum HiveTypeEnum {
    /**
     * sql解析
     */
    SELECT1 (1,"^(select)(.+)(from)(.+)"),SAVE1(2,"^(insert)(.+)"),DELETE1(3,"(delete from)(.+)"),
    CREATE1(4,"(create)(.+)"),TRUNCATE1(5,"(truncate)(.+)"),LOAD1(6,"(load)(.+)"),SET1(7,"^(set)(.+)")
    ,WITH1(8,"(with)(.+)"),SELECTNAME(9,"select"),INSERTNAME(10,"insert"),DELETENAME(11,"delete"),DDL(12,"ddl")
    ,DROP1(13,"(drop)(.+)"),HCATALOGMETASTOREURIS(14,"hive.metastore.uris"),HIVEDATABASE(15,"hivedatabase"),HIVETABLE(16,"hivetable");

    /**
     * hive参数
     */
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

    HiveTypeEnum() {
    }

    HiveTypeEnum(int value, String name) {
        this.value = value;
        this.name = name;
    }
}
