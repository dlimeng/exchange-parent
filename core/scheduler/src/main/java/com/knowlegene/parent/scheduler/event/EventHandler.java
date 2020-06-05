package com.knowlegene.parent.scheduler.event;

/**
 * @Classname EventHandler
 * @Description TODO
 * @Date 2020/6/5 15:44
 * @Created by limeng
 */
public interface EventHandler<T extends Event> {
    void handle(T event);
}
