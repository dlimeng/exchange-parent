package com.knowlegene.parent.process.runners;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.model.SwapOptions;
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

    public void start(){
        this.startRunners();
        this.initApplication();
        this.setJobStream();
    }

    public void start(Class<? extends PipelineOptions> clazz,String[] args){
        this.startRunnersByArgs(clazz,args);
        argsAndOptions(this.getPipelineOptions().as(clazz));
        this.initApplication();
        this.setJobStream();
    }

    public void start(String[] args){
        this.startRunnersByArgs(SwapPipelineOptions.class,args);
        argsAndOptions(this.getPipelineOptions().as(SwapPipelineOptions.class));
        this.initApplication();
        this.setJobStream();
    }

    /**
     * 参数
     */
    public void argsAndOptions(Object pipelineOptions){
        if(swapOptions == null && pipelineOptions!=null){
            SwapOptions newSwap = new SwapOptions();
            BaseUtil.copyNonNullProperties(newSwap,pipelineOptions);
            this.swapOptions = newSwap;
        }
    }

    /**
     * 导入
     */
    public void getImport(){
        if(swapOptions != null){
            if(isImport()){
                new BaseSwapTool(swapOptions).run(BaseSwapTool.isImport);
            }
        }
    }

    /**
     * 是否导入
     * @return
     */
    public boolean isImport(){
        boolean result=false;
        Boolean importStatus = swapOptions.getImportOptions();
        if(importStatus != null){
            if(importStatus){
                result = true;
            }
        }
        return result;
    }

    /**
     * 是否导出
     * @return
     */
    public boolean isExport(){
        boolean result=false;
        Boolean exportStatus = swapOptions.getExportOptions();
        if(exportStatus != null){
            if(exportStatus){
                result = true;
            }
        }
        return result;
    }

    public void getExport(){
        if(swapOptions != null){
            if(isExport()){
                new BaseSwapTool(swapOptions).run(BaseSwapTool.isExport);
            }
        }
    }

    /**
     * 执行计划
     */
    public abstract void  setJobStream();



    public SwapRunners(){

    }
}
