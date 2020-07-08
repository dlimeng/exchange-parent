package com.knowlegene.parent.process.io.neo4j;

import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jObject;

import java.util.Map;
import java.util.Set;

/**
 * @Author: limeng
 * @Date: 2019/9/23 17:48
 */
public interface Neo4jSwap {
    /**
     *
     * @param statement
     * @return
     */
    Neo4jIO.Write<Neo4jObject> write(String statement);

    Neo4jIO.Write<Neo4jObject> relate(String statement, String label);

    Neo4jIO.Read<Neo4jObject> query(String sql);


    Neo4jIO.Read<Set<Map<String, ObjectCoder>>> query(String sql, String type);
}
