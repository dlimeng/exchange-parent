package com.knowlegene.parent.process.swap.event;

import com.knowlegene.parent.config.common.event.MySQLExportType;
import com.knowlegene.parent.scheduler.event.AbstractEvent;

/**
 * @Classname HiveExportTaskEvent
 * @Description TODO
 * @Date 2020/6/11 15:53
 * @Created by limeng
 */
public class MySQLExportTaskEvent extends AbstractEvent<MySQLExportType> {


    public MySQLExportTaskEvent(MySQLExportType exportEnum) {
        super(exportEnum);
    }

    public MySQLExportTaskEvent(MySQLExportType exportEnum, long timestamp) {
        super(exportEnum, timestamp);
    }
}
