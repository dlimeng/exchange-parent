package com.knowlegene.parent.process.io.neo4j.impl;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.common.constantenum.DataSourceEnum;
import com.knowlegene.parent.process.common.constantenum.Neo4jEnum;
import com.knowlegene.parent.process.io.neo4j.Neo4jIO;
import com.knowlegene.parent.process.io.neo4j.Neo4jSwap;
import com.knowlegene.parent.process.model.neo4j.Neo4jObject;
import com.knowlegene.parent.process.util.Neo4jDataSourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @Author: limeng
 * @Date: 2019/9/23 17:49
 */
public class Neo4jSwapImpl implements Neo4jSwap, Serializable {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public Neo4jIO.Write<Neo4jObject> write(String statement) {
        if(BaseUtil.isNotBlank(statement)){
            String url = DataSourceEnum.NEOURL.getName();
            String username = DataSourceEnum.NEOUSERNAME.getName();
            String password = DataSourceEnum.NEOPASSWORD.getName();

            return Neo4jIO.<Neo4jObject>write()
                    .withDriverConfiguration(Neo4jIO.DriverConfiguration
                            .create(get(url),get(username),get(password)))
                    .withStatement(statement);
        }else{
            logger.info("statement is null");
        }
        return null;
    }

    @Override
    public Neo4jIO.Write<Neo4jObject> relate(String statement, String label) {
        if(BaseUtil.isNotBlank(statement) && BaseUtil.isNotBlank(label)){
            String url = DataSourceEnum.NEOURL.getName();
            String username = DataSourceEnum.NEOUSERNAME.getName();
            String password = DataSourceEnum.NEOPASSWORD.getName();

            return Neo4jIO.<Neo4jObject>write()
                    .withDriverConfiguration(Neo4jIO.DriverConfiguration
                            .create(get(url),get(username),get(password)))
                    .withStatement(statement).withLabel(label).withOptionsType(Neo4jEnum.RELATE.getName());
        }else{
            logger.info("statement is null");
        }
        return null;
    }

    private String get(String key){
        if(BaseUtil.isNotBlank(key)){
            return Neo4jDataSourceUtil.get(key).toString();
        }
        return null;
    }
}
