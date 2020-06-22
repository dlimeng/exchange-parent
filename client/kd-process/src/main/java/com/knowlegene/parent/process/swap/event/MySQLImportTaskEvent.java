package com.knowlegene.parent.process.swap.event;


import com.knowlegene.parent.config.common.event.MySQLImportType;
import com.knowlegene.parent.scheduler.event.AbstractEvent;

/**
 * @Classname HiveTaskEvent
 * @Description TODO
 * @Date 2020/6/11 15:29
 * @Created by limeng
 */
public class MySQLImportTaskEvent extends AbstractEvent<MySQLImportType> {

    public MySQLImportTaskEvent(MySQLImportType importEnum) {
        super(importEnum);
    }
}
