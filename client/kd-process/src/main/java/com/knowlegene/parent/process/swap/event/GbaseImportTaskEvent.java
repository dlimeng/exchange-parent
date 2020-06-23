package com.knowlegene.parent.process.swap.event;


import com.knowlegene.parent.config.common.event.GbaseImportType;
import com.knowlegene.parent.scheduler.event.AbstractEvent;

/**
 * @Classname
 * @Description TODO
 * @Date 2020/6/11 15:29
 * @Created by limeng
 */
public class GbaseImportTaskEvent extends AbstractEvent<GbaseImportType> {

    public GbaseImportTaskEvent(GbaseImportType importEnum) {
        super(importEnum);
    }
}
