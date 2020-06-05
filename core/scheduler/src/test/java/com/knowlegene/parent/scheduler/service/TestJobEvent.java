package com.knowlegene.parent.scheduler.service;

import com.knowlegene.parent.scheduler.event.AbstractEvent;

/**
 * @Classname TestJobEvent
 * @Description TODO
 * @Date 2020/6/5 16:41
 * @Created by limeng
 */
public class TestJobEvent extends AbstractEvent<TestJobEventType> {
    private String jobID;

    public TestJobEvent (String jobID, TestJobEventType type) {
        super(type);
        this.jobID = jobID;
    }

    public TestJobEvent(TestJobEventType testJobEventType) {
        super(testJobEventType);
    }

    public TestJobEvent(TestJobEventType testJobEventType, long timestamp) {
        super(testJobEventType, timestamp);
    }
}
