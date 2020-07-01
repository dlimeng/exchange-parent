package com.knowlegene.parent.process.runners.common;


import com.knowlegene.parent.process.swap.dispatcher.SwapMaster;
import com.knowlegene.parent.process.util.JobThreadLocalUtil;
import com.knowlegene.parent.process.util.SwapMasterUtil;
import com.knowlegene.parent.process.util.UUIDFactoryUtil;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/8/23 15:35
 */
public abstract class BaseJobRunners implements Serializable {
    private final Logger logger;
    private BaseRunners baseRunners;
    public Logger getLogger() {
        return logger;
    }

    public BaseJobRunners() {
        this.logger = LoggerFactory.getLogger(getClass());
        String uuidStr = UUIDFactoryUtil.getUUIDStr();
        JobThreadLocalUtil.setJob(uuidStr);
    }

    /**
     * pipline开始
     */
    public void startRunners(){
        baseRunners = new BaseRunners();
        baseRunners.start();
    }

    /**
     * pipline开始
     */
    public void startRunners(Class<? extends PipelineOptions> clazz, Map<String, Object> map){
        baseRunners = new BaseRunners(clazz);
        baseRunners.start(map);
    }

    /**
     * pipline开始
     */
    public void startRunnersByArgs(Class<? extends PipelineOptions> clazz, String[] args){
        baseRunners = new BaseRunners(clazz);
        baseRunners.startByArgs(args);
    }

    public PipelineOptions getPipelineOptions(){
        if(baseRunners != null){
            return baseRunners.getPipelineOptions();
        }
        return null;
    }

    /**
     * 执行dag
     */
    public void run(){
        baseRunners.run();
    }


    protected static SwapMaster getSwapMaster(){
        return SwapMasterUtil.instance;
    }

    protected abstract void init();
}
