package com.knowlegene.parent.process.util;

import com.knowlegene.parent.process.swap.dispatcher.SwapMaster;
import com.knowlegene.parent.scheduler.event.Dispatcher;

import java.io.Serializable;

/**
 * @Classname DispatcherUtil
 * @Description TODO
 * @Date 2020/6/11 16:28
 * @Created by limeng
 */
public class SwapMasterUtil implements Serializable {

    private SwapMasterUtil() {
    }

    public static volatile SwapMaster instance  = null;

    public static SwapMaster getInstance(SwapMaster newPipeline){
        if(instance == null){
            synchronized (Dispatcher.class){
                if(instance == null && newPipeline != null){
                    instance = newPipeline;
                }
            }
        }
        return instance;
    }

}
