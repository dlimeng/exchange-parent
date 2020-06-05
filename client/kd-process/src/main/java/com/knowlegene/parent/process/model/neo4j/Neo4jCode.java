package com.knowlegene.parent.process.model.neo4j;

import lombok.Data;

/**
 * @Author: limeng
 * @Date: 2019/9/23 19:49
 */
@Data
public class Neo4jCode {
    private String id;
    private String node;
    private String relation;
    private String property;
    private String label;

    private String nodeFromId;
    private String nodeFromLabel;
    private String nodeToId;
    private String nodeToLabel;

    private String where;
    private String update;
    private String result;
    private String optionType;




}
