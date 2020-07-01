package com.knowlegene.parent.process.pojo.neo4j;

import com.knowlegene.parent.process.common.annotation.StoredAsProperty;
import lombok.Data;

/**
 * @Author: limeng
 * @Date: 2019/9/23 19:49
 */
@Data
public class Neo4jOptions {
    @StoredAsProperty("neo4j.cypher")
    private String cypher;
    @StoredAsProperty("neo4j.neourl")
    private String neoUrl;
    @StoredAsProperty("neo4j.neousername")
    private String neoUsername;
    @StoredAsProperty("neo4j.neopassword")
    private String neoPassword;
    @StoredAsProperty("neo4j.neoformat")
    private String neoFormat;
    @StoredAsProperty("neo4j.label")
    private String neoLabel;
    @StoredAsProperty("neo4j.type")
    private String neoType;



}
