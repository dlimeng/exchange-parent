package com.knowlegene.parent.process.swap;


import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.event.HiveExportType;
import com.knowlegene.parent.config.common.event.HiveImportType;
import com.knowlegene.parent.config.common.event.SwapJobEventType;
import com.knowlegene.parent.config.util.PipelineSingletonUtil;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.pojo.hive.HiveOptions;
import com.knowlegene.parent.process.swap.dispatcher.SwapMaster;
import com.knowlegene.parent.process.swap.common.BaseSwap;
import com.knowlegene.parent.process.swap.event.HiveExportTaskEvent;
import com.knowlegene.parent.process.swap.event.HiveImportTaskEvent;
import com.knowlegene.parent.process.swap.event.SwapJobEvent;
import com.knowlegene.parent.process.tool.BaseSwapTool;
import com.knowlegene.parent.process.util.SwapMasterUtil;
import com.knowlegene.parent.scheduler.event.AbstractEvent;
import com.knowlegene.parent.scheduler.event.EventHandler;
import com.knowlegene.parent.scheduler.service.Service;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import lombok.Data;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: limeng
 * @Date: 2019/8/20 15:54
 */
@Data
public class JobBase extends BaseSwap {
    protected static SwapOptions options;

    private static Logger logger = null;

    private static String fromPre = "from_";
    private static String toSuffix = "to_";

    public JobBase() {
        JobBase.logger =  LoggerFactory.getLogger(this.getClass());
    }

    public JobBase(SwapOptions options) {
        JobBase.logger =  LoggerFactory.getLogger(this.getClass());
        JobBase.options = options;
    }


    public static Logger getLogger(){
        return logger;
    }


    protected static Object getOptions(String keys){
        boolean exist = CacheManager.isExist(keys);
        Object result = null;
        if(!exist) return result;
        result = CacheManager.getCache(keys);
        return result;
    }



    public static class SwapJobEventDispatcher implements EventHandler<SwapJobEvent> {
        @Override
        public void handle(SwapJobEvent event) {

            if(event.getType() == SwapJobEventType.JOB_KILL){
                try {
                    getSwapMaster().serviceStop();
                    getSwapMaster().setAlive(false);
                } catch (Exception e) {
                    logger.error("SwapMaster stop msg:{}",e.getMessage());
                }
            }else if(event.getType() == SwapJobEventType.JOB_START){
                getPipeline().run().waitUntilFinish();
            }
        }
    }


    public void run(){
        getSwapMaster().getDispatcher().getEventHandler().handle((AbstractEvent)BaseSwapTool.getTask(fromPre + options.getFromName()));
        getSwapMaster().getDispatcher().getEventHandler().handle((AbstractEvent)BaseSwapTool.getTask(toSuffix + options.getToName()));
        getSwapMaster().getDispatcher().getEventHandler().handle(new SwapJobEvent(SwapJobEventType.JOB_START));
        getSwapMaster().getDispatcher().getEventHandler().handle(new SwapJobEvent(SwapJobEventType.JOB_KILL));
    }

    protected static Pipeline getPipeline(){
        return PipelineSingletonUtil.instance;
    }

    protected static SwapMaster getSwapMaster(){
        return SwapMasterUtil.instance;
    }
}
