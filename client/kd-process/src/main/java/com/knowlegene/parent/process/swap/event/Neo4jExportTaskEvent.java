package com.knowlegene.parent.process.swap.event;

import com.knowlegene.parent.config.common.event.Neo4jExportType;
import com.knowlegene.parent.scheduler.event.AbstractEvent;

/**
 * @Classname
 * @Description TODO
 * @Date 2020/6/11 15:53
 * @Created by limeng
 */
public class Neo4jExportTaskEvent extends AbstractEvent<Neo4jExportType> {


    public Neo4jExportTaskEvent(Neo4jExportType exportEnum) {
        super(exportEnum);
    }

}
