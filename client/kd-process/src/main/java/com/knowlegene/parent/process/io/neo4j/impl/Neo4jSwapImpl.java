package com.knowlegene.parent.process.io.neo4j.impl;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.common.constantenum.Neo4jEnum;
import com.knowlegene.parent.process.io.neo4j.Neo4jIO;
import com.knowlegene.parent.process.io.neo4j.Neo4jSwap;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jObject;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jOptions;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

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


    @Override
    public Neo4jIO.Read<Set<Map<String, ObjectCoder>>> query(String sql, String type) {
        Neo4jOptions driver = getDriver();

        if(driver != null && BaseUtil.isNotBlank(sql) && BaseUtil.isNotBlank(type)){

            return Neo4jIO.<Set<Map<String, ObjectCoder>>>read()
                    .withDriverConfiguration(Neo4jIO.DriverConfiguration.create(driver.getNeoUrl(),driver.getNeoUsername(), driver.getNeoPassword()))
                    .withQuery(sql)
                    .withCoder(SetCoder.of(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ObjectCoder.class))))
                    .withRowMapper(new Neo4jIO.RowMapper<Set<Map<String, ObjectCoder>>>(){
                        @Override
                        public Set<Map<String, ObjectCoder>> mapRow(StatementResult resultSet) throws Exception {
                            Record record = resultSet.next();
                            Map<String, Object> date = record.asMap();
                            Set<Map<String, ObjectCoder>> result = new HashSet<>();
                            Map<String, ObjectCoder> datamap = null;
                            Object object = null;
                            InternalNode dataNode = null;
                            InternalPath dataPath = null;
                            Iterator<Node> nodes = null;
                            for(String key : date.keySet()){
                                object = date.get(key);
                                if(object == null) continue;

                                if(Neo4jEnum.NODE.getName().equalsIgnoreCase(type)){
                                      if(object instanceof InternalNode){
                                          dataNode = (InternalNode) object;
                                          Map<String, Object> nodeMap = dataNode.asMap();
                                          if(!BaseUtil.isBlankMap(nodeMap)){
                                              long nodeId = dataNode.id();
                                              datamap = new HashMap<>();
                                              for(Map.Entry<String, Object> map : nodeMap.entrySet()){
                                                  datamap.put(map.getKey(), new ObjectCoder(map.getValue()));
                                              }
                                              datamap.put(Neo4jEnum.NODE_ID.getName(), new ObjectCoder(String.valueOf(nodeId)));
                                              result.add(datamap);
                                          }
                                      }else if(object instanceof InternalPath){
                                          dataPath = (InternalPath) object;
                                          nodes = dataPath.nodes().iterator();
                                          while (nodes.hasNext()){
                                                Node node = nodes.next();
                                                long nodeId = node.id();
                                                datamap = new HashMap<>();
                                                // 添加节点的属性
                                                Map<String, Object> data1 = node.asMap();
                                                for (String key1 : data1.keySet()) {
                                                    datamap.put(key1, new ObjectCoder(data1.get(key1)));
                                                }
                                                datamap.put(Neo4jEnum.NODE_ID.getName(), new ObjectCoder(String.valueOf(nodeId)));
                                                result.add(datamap);
                                          }
                                      }


                                }else if(Neo4jEnum.RELATE.getName().equalsIgnoreCase(type)){
                                    dataPath = (InternalPath) object;
                                    Iterator<Relationship> relationships = dataPath.relationships().iterator();
                                    while (relationships.hasNext()){
                                        datamap = new HashMap<>();
                                        Relationship relationship = relationships.next();
                                        Map<String, Object> data1 = relationship.asMap();// 添加关系的属性
                                        for (String key1 : data1.keySet()) {
                                            datamap.put(key1, new ObjectCoder(data1.get(key1)));
                                        }
                                        long source = relationship.startNodeId();// 起始节点id
                                        long target = relationship.endNodeId();// 结束节点Id
                                        datamap.put(Neo4jEnum.START_ID.getName(), new ObjectCoder(String.valueOf(source)));// 添加起始节点id
                                        datamap.put(Neo4jEnum.END_ID.getName(),  new ObjectCoder(String.valueOf(target)));
                                        result.add(datamap);
                                    }
                                }
                            }
                            return result;
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
