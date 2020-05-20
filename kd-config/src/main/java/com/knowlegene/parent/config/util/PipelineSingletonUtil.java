package com.knowlegene.parent.config.util;

import org.apache.beam.sdk.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @Author: limeng
 * @Date: 2019/7/18 16:18
 */
public class PipelineSingletonUtil  implements Serializable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    public PipelineSingletonUtil() {
    }

    public static volatile Pipeline instance  = null;

    public static Pipeline getInstance(Pipeline newPipeline){
        if(instance == null){
            synchronized (PipelineSingletonUtil.class){
                if(instance == null && newPipeline != null){
                    instance = newPipeline;
                }
            }
        }
        return instance;
    }
}
