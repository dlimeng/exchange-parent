package com.knowlegene.parent.process.runners;

import com.knowlegene.parent.config.util.PipelineSingletonUtil;
import com.knowlegene.parent.config.util.ProxyFactoryUtil;
import com.knowlegene.parent.process.extract.dag.hive.BaseETLDefaultMethod;
import com.knowlegene.parent.process.runners.common.BaseJobRunners;
import com.knowlegene.parent.process.runners.options.IndexerPipelineOptions;
import com.knowlegene.parent.process.runners.options.SwapPipelineOptions;

import java.util.HashMap;
import java.util.Map;

/**
 * hive 流程处理
 * @Author: limeng
 * @Date: 2019/8/13 17:08
 */
public abstract class JobRunners extends BaseJobRunners {


    public JobRunners(){

    }

    /**
     * pipline开始
     */
    public void start(){
        this.startRunners();
        this.setJobStream();
    }

    public void start(String[] args){
        this.startRunnersByArgs(IndexerPipelineOptions.class,args);
        this.setJobStream();
    }


    /**
     * 执行dag
     */
    @Override
    public void run(){
        PipelineSingletonUtil.instance.run().waitUntilFinish();
    }

    /**
     * 执行计划
     */

    public abstract void  setJobStream();

    /**
     * hive etl 默认方法 ddl etl
     * @param defaultMethod
     */
    public void defaultMethod(Class<? extends BaseETLDefaultMethod> defaultMethod){
        try {
            BaseETLDefaultMethod method = defaultMethod.newInstance();
            BaseETLDefaultMethod proxyInstance = (BaseETLDefaultMethod)new ProxyFactoryUtil(method).getProxyInstance();
            proxyInstance.ddl();
            proxyInstance.etl();
        } catch (Exception e) {
            getLogger().error("defaultMethod is error=>msg:{}",e.getMessage());
        }
    }

    @Override
    public void init(){

    }

}
