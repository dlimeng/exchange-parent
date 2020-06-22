package com.knowlegene.parent.process.swap;


import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.event.HiveExportType;
import com.knowlegene.parent.config.common.event.HiveImportType;
import com.knowlegene.parent.config.common.event.SwapJobEventType;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.config.util.PipelineSingletonUtil;
import com.knowlegene.parent.config.util.ProxyFactoryUtil;
import com.knowlegene.parent.process.pojo.DBOptions;
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
    protected boolean isImport;
    protected boolean isExport;


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




    protected boolean isHiveImport(){
//        boolean ishive=false;
//        String hiveTableName = options.getHiveTableName();
//        if(BaseUtil.isNotBlank(hiveTableName)){
//            ishive = true;
//        }
//        return ishive;

        return false;
    }

    protected boolean isHiveExport(){
//        boolean ishive=false;
//        String hiveTableName = options.getHiveTableName();
//        if(BaseUtil.isNotBlank(hiveTableName)){
//           ishive = true;
//        }
//        return ishive;
        return false;
    }


    protected boolean isMySQL(){
//        boolean isMysql=false;
//        String driverClassName = options.getDriverClass();
//        if(BaseUtil.isNotBlank(driverClassName)){
//            isMysql = driverClassName.toLowerCase().contains("mysql");
//        }
//        return isMysql;
        return false;
    }
    protected boolean isOracle(){
//        boolean isoracle=false;
//        String driverClassName = options.getDriverClass();
//        if(BaseUtil.isNotBlank(driverClassName)){
//            isoracle = driverClassName.toLowerCase().contains("oracle");
//        }
//        return isoracle;
        return false;
    }

    protected boolean isGbase(){
//        boolean isGbase=false;
//        String driverClassName = options.getDriverClass();
//        if(BaseUtil.isNotBlank(driverClassName)){
//            isGbase = driverClassName.toLowerCase().contains("gbase");
//        }
//        return isGbase;

        return false;
    }

    protected boolean isEs(){
//        boolean result=true;
//        String[] esAddrs = options.getEsAddrs();
//        String esIndex = options.getEsIndex();
//        String esType = options.getEsType();
//        if(esAddrs == null || esAddrs.length == 0){
//            result = false;
//        }
//        if(BaseUtil.isBlank(esIndex)){
//            result = false;
//        }
//        if(BaseUtil.isBlank(esType)){
//            result = false;
//        }
//        return result;
        return true;
    }


    protected boolean isFilePath(){
//        boolean isPath=false;
//        String filePath = options.getFilePath();
//        String fieldDelim = options.getFieldDelim();
//        if(BaseUtil.isBlank(fieldDelim)){
//            options.setFieldDelim("\t");
//        }
//        if(BaseUtil.isNotBlank(filePath)){
//            isPath = true;
//        }
//        return isPath;

        return true;
    }

    protected boolean isNeo4j(){
//        boolean result=true;
//        String neoUrl = options.getNeoUrl();
//        String neoPassword = options.getNeoPassword();
//        String neoUsername = options.getNeoUsername();
//
//        if(BaseUtil.isBlank(neoUrl) || BaseUtil.isBlank(neoPassword) ||  BaseUtil.isBlank(neoUsername)){
//            result=false;
//            logger.error("url is null");
//        }
//        return result;

        return true;
    }



    protected ImportJobBase getImportJobBase(Class<? extends ImportJobBase> clazz){
//        try {
//            ImportJobBase importJobBase = clazz.newInstance();
//            ImportJobBase proxyInstance = (ImportJobBase) new ProxyFactoryUtil(importJobBase).getProxyInstance();
//            proxyInstance.setOptions(options);
//            return proxyInstance;
//        } catch (Exception e) {
//            logger.error("importJobBase is null");
//        }
        return null;
    }

    protected ExportJobBase getExportJobBase(Class<? extends ExportJobBase> clazz){
//        try {
//            ExportJobBase exportJobBase = clazz.newInstance();
//            ExportJobBase proxyInstance = (ExportJobBase) new ProxyFactoryUtil(exportJobBase).getProxyInstance();
//            proxyInstance.setOptions(options);
//            return proxyInstance;
//        } catch (Exception e) {
//            logger.error("exportJobBase is null");
//        }
        return null;
    }

    public boolean isSystemFile(){
        return true;
    }
    public boolean isHdfsFile(){
        return true;
    }


    protected Schema getOracleSchemas(){
//        String[] dbColumn = options.getDbColumn();
//        String tableName = options.getTableName();
//        //表所有列
//        Schema allSchema = this.getOracleSwap().desc(tableName);
//
//        getLogger().info("oracle=>tableName:{}",tableName);
//        return JdbcUtil.columnConversion(dbColumn, allSchema);

        return null;
    }






    protected Schema getGbaseSchemas(){
//        String[] dbColumn = options.getDbColumn();
//        String tableName = options.getTableName();
//        //表所有列
//        Schema allSchema = this.getGbaseSwap().desc(tableName);
//
//        getLogger().info("mysql=>tableName:{}",tableName);
//        return JdbcUtil.columnConversion(dbColumn, allSchema);

        return null;
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
