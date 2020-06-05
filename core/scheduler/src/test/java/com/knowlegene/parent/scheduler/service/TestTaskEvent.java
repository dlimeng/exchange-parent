package com.knowlegene.parent.scheduler.service;

import com.knowlegene.parent.scheduler.event.AbstractEvent;

/**
 * @Classname TestTaskEvent
 * @Description TODO
 * @Date 2020/6/5 16:42
 * @Created by limeng
 */
public class TestTaskEvent extends AbstractEvent<TestTaskEventType> {
    private String taskID;
    public String getTaskID() {
        return taskID;
    }

    public TestTaskEvent (String taskID, TestTaskEventType type) {
        super(type);
        this.taskID = taskID;
    }

    public TestTaskEvent(TestTaskEventType testTaskEventType) {
        super(testTaskEventType);
    }

    public TestTaskEvent(TestTaskEventType testTaskEventType, long timestamp) {
        super(testTaskEventType, timestamp);
    }
}
