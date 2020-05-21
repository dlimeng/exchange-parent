package com.knowlegene.parent.process.tool;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.model.SwapOptions;
import com.knowlegene.parent.process.util.*;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

/**
 * @Author: limeng
 * @Date: 2019/8/20 19:12
 */
@Data
public  class BaseSwapTool {
    private final Logger logger=LoggerFactory.getLogger(getClass());
    private SwapOptions options;
    public static String isImport = "import";
    public static String isExport = "export";

    public BaseSwapTool(SwapOptions options) {
        this.options = options;
        setDataSource();
    }

    public Logger getLogger() {
        return logger;
    }
    public BaseSwapTool() {

    }

    private static final Map<String, Class<? extends BaseSwapTool>> TOOLS;
    static {
        TOOLS = new TreeMap<String, Class<? extends BaseSwapTool>>();
        registerTool("import",ImportTool.class);
        registerTool("export",ExportTool.class);
    }

    private static void registerTool(String toolName,
                                     Class<? extends BaseSwapTool> cls) {
        Class<? extends BaseSwapTool> existing = TOOLS.get(toolName);
        if (null != existing) {
            // Already have a tool with this name. Refuse to start.
            throw new RuntimeException("A plugin is attempting to register a tool "
                    + "with name " + toolName + ", but this tool already exists ("
                    + existing.getName() + ")");
        }
        TOOLS.put(toolName, cls);
    }

    public void run(String toolName){
        try {
            if(BaseUtil.isNotBlank(toolName)){
                TOOLS.get(toolName).newInstance().run(options);
            }
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    private boolean isMySQL(){
        boolean isMysql=false;
        String driverClassName = options.getDriverClass();
        if(BaseUtil.isNotBlank(driverClassName)){
            isMysql = driverClassName.toLowerCase().contains("mysql");
        }
        return isMysql;
    }
    private boolean isOracle(){
        boolean isoracle=false;
        String driverClassName = options.getDriverClass();
        if(BaseUtil.isNotBlank(driverClassName)){
            isoracle = driverClassName.toLowerCase().contains("oracle");
        }
        return isoracle;
    }

    private boolean isHive(){
        boolean ishive=false;
        String driverClassName = options.getHiveClass();
        if(BaseUtil.isNotBlank(driverClassName)){
            ishive = driverClassName.toLowerCase().contains("hive");
        }
        return ishive;
    }

    private boolean isNeo4j(){
        boolean result=true;
        String neoUrl = options.getNeoUrl();
        String neoPassword = options.getNeoPassword();
        String neoUsername = options.getNeoUsername();

        if(BaseUtil.isBlank(neoUrl) || BaseUtil.isBlank(neoPassword) ||  BaseUtil.isBlank(neoUsername)){
            result=false;
        }
        return result;
    }

    private boolean isGbase(){
        boolean isGbase=false;
        String driverClassName = options.getDriverClass();
        if(BaseUtil.isNotBlank(driverClassName)){
            isGbase = driverClassName.toLowerCase().contains("gbase");
        }
        return isGbase;
    }

    /**
     * 设置源
     */
    private void setDataSource(){
        if(isMySQL()){
            MySQLDataSourceUtil.getSessionFactoryInstance(options);
        }
        if(isOracle()){
            OracleDataSourceUtil.getSessionFactoryInstance(options);
        }
        if(isHive()){
            HiveDataSourceUtil.getSessionFactoryInstance(options);
        }
        if(isNeo4j()){
            Neo4jDataSourceUtil.getSessionFactoryInstance(options);
        }
        if(isGbase()){
            GbaseDataSourceUtil.getSessionFactoryInstance(options);
        }
    }



    public void run(SwapOptions swapOptions){

    }

}
