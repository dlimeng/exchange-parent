package com.knowlegene.parent.process.io.neo4j;

import com.knowlegene.parent.process.model.neo4j.Neo4jObject;

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
}
