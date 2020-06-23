package com.knowlegene.parent.process.swap.event;

import com.knowlegene.parent.config.common.event.GbaseExportType;
import com.knowlegene.parent.scheduler.event.AbstractEvent;

/**
 * @Classname
 * @Description TODO
 * @Date 2020/6/11 15:53
 * @Created by limeng
 */
public class GbaseExportTaskEvent extends AbstractEvent<GbaseExportType> {


    public GbaseExportTaskEvent(GbaseExportType exportEnum) {
        super(exportEnum);
    }

}
