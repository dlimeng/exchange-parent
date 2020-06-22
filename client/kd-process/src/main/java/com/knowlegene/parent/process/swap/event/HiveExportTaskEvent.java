package com.knowlegene.parent.process.swap.event;

import com.knowlegene.parent.config.common.event.HiveExportType;
import com.knowlegene.parent.scheduler.event.AbstractEvent;
/**
 * @Classname HiveExportTaskEvent
 * @Description TODO
 * @Date 2020/6/11 15:53
 * @Created by limeng
 */
public class HiveExportTaskEvent extends AbstractEvent<HiveExportType> {


    public HiveExportTaskEvent(HiveExportType hiveExportEnum) {
        super(hiveExportEnum);
    }

    public HiveExportTaskEvent(HiveExportType hiveExportEnum, long timestamp) {
        super(hiveExportEnum, timestamp);
    }
}
