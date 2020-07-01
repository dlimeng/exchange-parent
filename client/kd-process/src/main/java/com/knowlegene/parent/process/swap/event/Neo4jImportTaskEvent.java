package com.knowlegene.parent.process.swap.event;


import com.knowlegene.parent.config.common.event.Neo4jImportType;
import com.knowlegene.parent.scheduler.event.AbstractEvent;

/**
 * @Classname
 * @Description TODO
 * @Date 2020/6/11 15:29
 * @Created by limeng
 */
public class Neo4jImportTaskEvent extends AbstractEvent<Neo4jImportType> {

    public Neo4jImportTaskEvent(Neo4jImportType importEnum) {
        super(importEnum);
    }
}
