package com.knowlegene.parent.config.common.constantenum;

/**
 * @Classname DBOperationEnum
 * @Description TODO
 * @Date 2020/6/10 14:07
 * @Created by limeng
 */
public enum DBOperationEnum {
    /**
     * 各个源操作
     */
    HIVE_IMPORT_DB(1,"HIVE_IMPORT_DB"),HIVE_EXPORT_DB(2,"HIVE_EXPORT_DB"),HIVE_IMPORT(41,"HIVE_IMPORT"),HIVE_EXPORT(42,"HIVE_EXPORT"),
    MYSQL_IMPORT_DB(3,"MYSQL_IMPORT_DB"),MYSQL_EXPORT_DB(4,"MYSQL_EXPORT_DB"),MYSQL_IMPORT(43,"MYSQL_IMPORT"),MYSQL_EXPORT(44,"MYSQL_EXPORT"),
    ORACLE_IMPORT_DB(5,"ORACLE_IMPORT_DB"),ORACLE_EXPORT_DB(6,"ORACLE_EXPORT_DB"),ORACLE_IMPORT(45,"ORACLE_IMPORT"),ORACLE_EXPORT(46,"ORACLE_EXPORT"),
    GBASE_IMPORT_DB(7,"GBASE_IMPORT_DB"),GBASE_EXPORT_DB(8,"GBASE_EXPORT_DB"),GBASE_IMPORT(47,"GBASE_IMPORT"),GBASE_EXPORT(48,"GBASE_EXPORT"),
                                                                                                        ES_IMPORT(49,"ES_IMPORT"),ES_EXPORT(50,"ES_EXPORT"),
                                                                                                        FILE_IMPORT(51,"FILE_IMPORT"),FILE_EXPORT(52,"FILE_EXPORT"),
                                                                                                        NEO4J_IMPORT(53,"NEO4J_IMPORT"),NEO4J_EXPORT(54,"NEO4J_EXPORT"),


    PCOLLECTION_QUERYS(300,"PCOLLECTION_QUERYS"),IMPORT(400,"IMPORT"),EXPORT(401,"EXPORT");


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

    DBOperationEnum() {
    }

    DBOperationEnum(int value, String name) {
        this.value = value;
        this.name = name;
    }


    public static DBOperationEnum getEnum(String keys){
        DBOperationEnum[] values = DBOperationEnum.values();
        for(DBOperationEnum d:values){
            if(keys.equalsIgnoreCase(d.getName())) return d;
        }
        return null;
    }

}
