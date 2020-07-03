package com.knowlegene.parent.process.io.neo4j.impl;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.common.constantenum.Neo4jEnum;
import com.knowlegene.parent.process.io.neo4j.Neo4jIO;
import com.knowlegene.parent.process.io.neo4j.Neo4jSwap;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jObject;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jOptions;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.neo4j.driver.v1.StatementResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @Author: limeng
 * @Date: 2019/9/23 17:49
 */
public abstract class Neo4jSwapImpl implements Neo4jSwap, Serializable {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public Neo4jIO.Write<Neo4jObject> write(String statement) {
        Neo4jOptions driver = getDriver();

        if(driver!=null && BaseUtil.isNotBlank(statement)){



            return Neo4jIO.<Neo4jObject>write()
                    .withDriverConfiguration(Neo4jIO.DriverConfiguration
                            .create(driver.getNeoUrl(),driver.getNeoUsername(), driver.getNeoPassword()))
                    .withStatement(statement);
        }else{
            logger.info("statement is null");
        }
        return null;
    }

    @Override
    public Neo4jIO.Write<Neo4jObject> relate(String statement, String label) {
        Neo4jOptions driver = getDriver();

        if(driver != null && BaseUtil.isNotBlank(statement) && BaseUtil.isNotBlank(label)){

            return Neo4jIO.<Neo4jObject>write()
                    .withDriverConfiguration(Neo4jIO.DriverConfiguration
                            .create(driver.getNeoUrl(),driver.getNeoUsername(), driver.getNeoPassword()))
                    .withStatement(statement).withLabel(label).withOptionsType(Neo4jEnum.RELATE.getName());
        }else{
            logger.info("statement is null");
        }
        return null;
    }


    @Override
    public Neo4jIO.Read<Neo4jObject> query(String sql) {
        Neo4jOptions driver = getDriver();

        if(driver != null && BaseUtil.isNotBlank(sql)){

            return Neo4jIO.<Neo4jObject>read()
                    .withDriverConfiguration(Neo4jIO.DriverConfiguration.create(driver.getNeoUrl(),driver.getNeoUsername(), driver.getNeoPassword()))
                    .withQuery(sql)
                    .withCoder(SerializableCoder.of(Neo4jObject.class))
                    .withRowMapper(new Neo4jIO.RowMapper<Neo4jObject>(){
                        @Override
                        public Neo4jObject mapRow(StatementResult resultSet) throws Exception {
                            return new Neo4jObject(resultSet.list().toArray());
                        }
                    });

        }
        return null;
    }

    public abstract Neo4jOptions getDriver();

    protected static Object getOptions(String keys){
        boolean exist = CacheManager.isExist(keys);
        Object result = null;
        if(!exist) return result;
        result = CacheManager.getCache(keys);
        return result;
    }

}
