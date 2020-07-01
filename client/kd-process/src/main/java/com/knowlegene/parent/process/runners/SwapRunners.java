package com.knowlegene.parent.process.runners;

import com.knowlegene.parent.config.common.constantenum.DatabaseTypeEnum;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.runners.options.SwapPipelineOptions;
import com.knowlegene.parent.process.runners.common.BaseJobRunners;
import com.knowlegene.parent.process.tool.BaseSwapTool;
import lombok.Data;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据交换处理流程
 * @Author: limeng
 * @Date: 2019/8/21 13:55
 */
@Data
public abstract class SwapRunners extends BaseJobRunners {
    private  Logger logger = LoggerFactory.getLogger(getClass());
    private SwapOptions swapOptions;

    private Object swapObject =new Object();

    public void start(){
        this.startRunners();
        this.init();
        this.setJobStream();
        this.join();

    }

    public void start(Class<? extends PipelineOptions> clazz,String[] args){
        this.startRunnersByArgs(clazz,args);
        argsAndOptions(this.getPipelineOptions().as(clazz));
        this.init();
        this.setJobStream();
        this.join();
    }

    public void start(String[] args){
        this.startRunnersByArgs(SwapPipelineOptions.class,args);
        argsAndOptions(this.getPipelineOptions().as(SwapPipelineOptions.class));
        this.init();
        this.setJobStream();
        this.join();
    }

    /**
     * 参数
     */
    private void argsAndOptions(Object pipelineOptions){
        if(swapOptions == null && pipelineOptions!=null){
            SwapOptions newSwap = new SwapOptions();
            BaseUtil.copyNonNullProperties(newSwap,pipelineOptions);
            this.swapOptions = newSwap;
        }
    }

    @Override
    protected void init(){
        if(swapOptions != null){
            String fromName = swapOptions.getFromName();
            String toName = swapOptions.getToName();
            Integer fromVal = DatabaseTypeEnum.queryValue(fromName).getValue();
            Integer toVal = DatabaseTypeEnum.queryValue(toName).getValue();
            if(fromVal==null || toVal==null){
                logger.error("fromName or toName is null");
            }
            try {
                new BaseSwapTool(swapOptions).run();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private  void join(){
        while (getSwapMaster().isAlive()){}
    }
    /**
     * 执行计划
     */
    public abstract void setJobStream();

    public SwapRunners(){

    }
}
