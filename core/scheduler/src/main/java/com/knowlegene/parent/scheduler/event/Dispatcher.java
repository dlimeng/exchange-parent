package com.knowlegene.parent.scheduler.event;

/**
 * @Classname Dispatcher
 * @Description TODO
 * @Date 2020/6/5 15:45
 * @Created by limeng
 */
public interface Dispatcher {

    public static final boolean DEFAULT_DISPATCHER_EXIT_ON_ERROR = false;

    EventHandler getEventHandler();

    void register(Class<? extends Enum> eventType, EventHandler handler);

}
