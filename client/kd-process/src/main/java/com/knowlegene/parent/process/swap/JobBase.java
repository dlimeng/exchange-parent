package com.knowlegene.parent.process.swap;


import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.config.util.PipelineSingletonUtil;
import com.knowlegene.parent.config.util.ProxyFactoryUtil;
import com.knowlegene.parent.process.model.SwapOptions;
import com.knowlegene.parent.process.swap.common.BaseSwap;
import lombok.Data;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: limeng
 * @Date: 2019/8/20 15:54
 */
@Data
public class JobBase extends BaseSwap {
    protected SwapOptions options;
    protected boolean isImport;
    protected boolean isExport;

    private Logger logger = null;

    public JobBase() {
        this.logger =  LoggerFactory.getLogger(this.getClass());
    }

    public JobBase(SwapOptions options) {
        this.logger =  LoggerFactory.getLogger(this.getClass());
        this.options = options;
    }


    public Logger getLogger(){
        return logger;
    }



    protected boolean isHiveImport(){
        boolean ishive=false;
        String hiveTableName = options.getHiveTableName();
        if(BaseUtil.isNotBlank(hiveTableName)){
            ishive = true;
        }
        return ishive;
    }

    protected boolean isHiveExport(){
        boolean ishive=false;
        String hiveTableName = options.getHiveTableName();
        if(BaseUtil.isNotBlank(hiveTableName)){
           ishive = true;
        }
        return ishive;
    }


    protected boolean isMySQL(){
        boolean isMysql=false;
        String driverClassName = options.getDriverClass();
        if(BaseUtil.isNotBlank(driverClassName)){
            isMysql = driverClassName.toLowerCase().contains("mysql");
        }
        return isMysql;
    }
    protected boolean isOracle(){
        boolean isoracle=false;
        String driverClassName = options.getDriverClass();
        if(BaseUtil.isNotBlank(driverClassName)){
            isoracle = driverClassName.toLowerCase().contains("oracle");
        }
        return isoracle;
    }

    protected boolean isGbase(){
        boolean isGbase=false;
        String driverClassName = options.getDriverClass();
        if(BaseUtil.isNotBlank(driverClassName)){
            isGbase = driverClassName.toLowerCase().contains("gbase");
        }
        return isGbase;
    }

    protected boolean isEs(){
        boolean result=true;
        String[] esAddrs = options.getEsAddrs();
        String esIndex = options.getEsIndex();
        String esType = options.getEsType();
        if(esAddrs == null || esAddrs.length == 0){
            result = false;
        }
        if(BaseUtil.isBlank(esIndex)){
            result = false;
        }
        if(BaseUtil.isBlank(esType)){
            result = false;
        }
        return result;
    }


    protected boolean isFilePath(){
        boolean isPath=false;
        String filePath = options.getFilePath();
        String fieldDelim = options.getFieldDelim();
        if(BaseUtil.isBlank(fieldDelim)){
            options.setFieldDelim("\t");
        }
        if(BaseUtil.isNotBlank(filePath)){
            isPath = true;
        }
        return isPath;
    }

    protected boolean isNeo4j(){
        boolean result=true;
        String neoUrl = options.getNeoUrl();
        String neoPassword = options.getNeoPassword();
        String neoUsername = options.getNeoUsername();

        if(BaseUtil.isBlank(neoUrl) || BaseUtil.isBlank(neoPassword) ||  BaseUtil.isBlank(neoUsername)){
            result=false;
            logger.error("url is null");
        }
        return result;
    }

    /**
     * isHCatalogIOStatus
     * @return
     */
    protected boolean isHCatalogIOStatus(){
        String metastoreHostName = options.getHMetastoreHost();
        String metastorePort = options.getHMetastorePort();
        String hiveDatabase = options.getHiveDatabase();
        String hiveTableName = options.getHiveTableName();
        if(BaseUtil.isBlank(metastoreHostName) || BaseUtil.isBlank(metastorePort) || BaseUtil.isBlank(hiveDatabase) || BaseUtil.isBlank(hiveTableName)){
            return false;
        }
        return true;
    }

    protected ImportJobBase getImportJobBase(Class<? extends ImportJobBase> clazz){
        try {
            ImportJobBase importJobBase = clazz.newInstance();
            ImportJobBase proxyInstance = (ImportJobBase) new ProxyFactoryUtil(importJobBase).getProxyInstance();
            proxyInstance.setOptions(options);
            return proxyInstance;
        } catch (Exception e) {
            logger.error("importJobBase is null");
        }
        return null;
    }

    protected ExportJobBase getExportJobBase(Class<? extends ExportJobBase> clazz){
        try {
            ExportJobBase exportJobBase = clazz.newInstance();
            ExportJobBase proxyInstance = (ExportJobBase) new ProxyFactoryUtil(exportJobBase).getProxyInstance();
            proxyInstance.setOptions(options);
            return proxyInstance;
        } catch (Exception e) {
            logger.error("exportJobBase is null");
        }
        return null;
    }

    public boolean isSystemFile(){
        return true;
    }
    public boolean isHdfsFile(){
        return true;
    }


    protected Schema getOracleSchemas(){
        String[] dbColumn = options.getDbColumn();
        String tableName = options.getTableName();
        //表所有列
        Schema allSchema = this.getOracleSwap().desc(tableName);

        getLogger().info("oracle=>tableName:{}",tableName);
        return JdbcUtil.columnConversion(dbColumn, allSchema);
    }

    protected Schema getMysqlSchemas(){
        String[] dbColumn = options.getDbColumn();
        String tableName = options.getTableName();
        //表所有列
        Schema allSchema = this.getMySQLSwap().desc(tableName);

        getLogger().info("mysql=>tableName:{}",tableName);
        return JdbcUtil.columnConversion(dbColumn, allSchema);
    }


    protected Schema getHiveSchemas(boolean isTimeStr){
        String tableName = options.getHiveTableName();
        String[] dbColumn = options.getHiveColumn();
        Schema allSchema = this.getHiveSwap().descByTableName(tableName,isTimeStr);
        getLogger().info("hive=>tableName:{}",tableName);
        return JdbcUtil.columnConversion(dbColumn, allSchema);
    }

    protected Schema getGbaseSchemas(){
        String[] dbColumn = options.getDbColumn();
        String tableName = options.getTableName();
        //表所有列
        Schema allSchema = this.getGbaseSwap().desc(tableName);

        getLogger().info("mysql=>tableName:{}",tableName);
        return JdbcUtil.columnConversion(dbColumn, allSchema);
    }


    protected Pipeline getPipeline(){
        return PipelineSingletonUtil.instance;
    }
}
