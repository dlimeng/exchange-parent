package com.knowlegene.parent.process.swap.event;


import com.knowlegene.parent.config.common.event.OracleImportType;
import com.knowlegene.parent.scheduler.event.AbstractEvent;

/**
 * @Classname
 * @Description TODO
 * @Date 2020/6/11 15:29
 * @Created by limeng
 */
public class OracleImportTaskEvent extends AbstractEvent<OracleImportType> {

    public OracleImportTaskEvent(OracleImportType importEnum) {
        super(importEnum);
    }
}
