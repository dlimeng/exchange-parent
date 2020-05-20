package com.knowlegene.parent.process.common.constantenum;

/**
 * @Author: limeng
 * @Date: 2019/8/12 14:46
 */
public interface OptionsEnum {
    public enum Indexer{
        PATTERN("pattern"),SOURCENAME("sourceName"),
        ISSQLFILE("sqlFile"),FILEPATH("filePath"),ISORDERLINK("orderLink")
        ,JDBCSQL("jdbcSql"),ISTREESTATUS("treeStatus");

        public final String name;

        Indexer(){
            this(null);
        }

        Indexer(String name){
            this.name = name;
        }
    }
}
