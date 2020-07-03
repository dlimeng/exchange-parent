package com.knowlegene.parent.process.io.neo4j.impl;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jOptions;

/**
 * @Classname Neo4jSwapExport
 * @Description TODO
 * @Date 2020/7/1 18:23
 * @Created by limeng
 */
public class Neo4jSwapExport extends Neo4jSwapImpl {
    @Override
    public Neo4jOptions getDriver() {
        String name = DBOperationEnum.NEO4J_EXPORT.getName();
        Object options = getOptions(name);
        if(options != null){
            return  (Neo4jOptions)options;
        }
        return null;
    }
}
