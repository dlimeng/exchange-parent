package com.knowlegene.parent.scheduler.event;

/**
 * @Classname Event
 * @Description TODO
 * @Date 2020/6/5 15:43
 * @Created by limeng
 */
public interface Event<TYPE extends Enum<TYPE>> {

    TYPE getType();
    long getTimestamp();
    String toString();
}
