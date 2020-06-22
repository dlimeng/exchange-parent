package com.knowlegene.parent.process.swap.event;


import com.knowlegene.parent.config.common.event.HiveImportType;
import com.knowlegene.parent.scheduler.event.AbstractEvent;

/**
 * @Classname HiveTaskEvent
 * @Description TODO
 * @Date 2020/6/11 15:29
 * @Created by limeng
 */
public class HiveImportTaskEvent extends AbstractEvent<HiveImportType> {

    public HiveImportTaskEvent(HiveImportType hiveImportEnum) {
        super(hiveImportEnum);
    }
}
