package com.knowlegene.parent.process.swap.event;


import com.knowlegene.parent.config.common.event.ESImportType;
import com.knowlegene.parent.scheduler.event.AbstractEvent;

/**
 * @Classname
 * @Description TODO
 * @Date 2020/6/11 15:29
 * @Created by limeng
 */
public class ESImportTaskEvent extends AbstractEvent<ESImportType> {

    public ESImportTaskEvent(ESImportType importEnum) {
        super(importEnum);
    }
}
