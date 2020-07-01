package com.knowlegene.parent.process.io.neo4j.impl;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jOptions;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

/**
 * @Classname Neo4jSwapExport
 * @Description TODO
 * @Date 2020/7/1 18:23
 * @Created by limeng
 */
public class Neo4jSwapExport extends Neo4jSwapImpl {
    @Override
    public Driver getDriver() {
        String name = DBOperationEnum.NEO4J_EXPORT.getName();
        Object options = getOptions(name);
        if(options != null){
            Neo4jOptions neo4jOptions = (Neo4jOptions)options;
            return GraphDatabase.driver(neo4jOptions.getNeoUrl(), AuthTokens.basic( neo4jOptions.getNeoUsername(), neo4jOptions.getNeoPassword()));
        }
        return null;
    }
}
