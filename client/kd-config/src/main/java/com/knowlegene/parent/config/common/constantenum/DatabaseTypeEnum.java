package com.knowlegene.parent.config.common.constantenum;

import com.knowlegene.parent.config.util.BaseUtil;

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
    ,HDFS(4,"hdfs"),HIVEDB1(5,"h1"),ES(6,"es"),ELASTICSEARCH(7,"elasticsearch"),GBASE(8,"gbase"),
    FILE(9,"file"),NEO4J(10,"neo4j");


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

    public static DatabaseTypeEnum queryValue(String name){
        DatabaseTypeEnum result = null;
        if(BaseUtil.isBlank(name)) return result;

        for(DatabaseTypeEnum d:DatabaseTypeEnum.values()){
            if(d.getName().equalsIgnoreCase(name)){
                return d;
            }
        }
        return result;
    }



    public static boolean isDB(String name){
        DatabaseTypeEnum databaseTypeEnum = queryValue(name);
        boolean result=false;
        switch (databaseTypeEnum){
            case MYSQL:
                result = true;
                break;
            case GBASE:
                result = true;
                break;
            case ORACLE:
                result = true;
                break;
        }
        return result;
    }


}
