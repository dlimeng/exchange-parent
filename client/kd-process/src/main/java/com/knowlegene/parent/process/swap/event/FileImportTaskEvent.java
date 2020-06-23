package com.knowlegene.parent.process.swap.event;



import com.knowlegene.parent.config.common.event.FileImportType;
import com.knowlegene.parent.scheduler.event.AbstractEvent;

/**
 * @Classname
 * @Description TODO
 * @Date 2020/6/11 15:29
 * @Created by limeng
 */
public class FileImportTaskEvent extends AbstractEvent<FileImportType> {

    public FileImportTaskEvent(FileImportType importEnum) {
        super(importEnum);
    }
}
