package com.knowlegene.parent.process.swap.event;

import com.knowlegene.parent.config.common.event.SwapJobEventType;
import com.knowlegene.parent.scheduler.event.AbstractEvent;

/**
 * @Classname SwapJobEvent
 * @Description TODO
 * @Date 2020/6/15 16:23
 * @Created by limeng
 */
public class SwapJobEvent extends AbstractEvent<SwapJobEventType>{
    public SwapJobEvent(SwapJobEventType swapJobEventType) {
        super(swapJobEventType);
    }

    public SwapJobEvent(SwapJobEventType swapJobEventType, long timestamp) {
        super(swapJobEventType, timestamp);
    }
}
