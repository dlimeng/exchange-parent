package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.event.OracleImportType;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.pojo.db.DBOptions;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.swap.event.OracleImportTaskEvent;
import com.knowlegene.parent.process.transform.TypeConversion;
import com.knowlegene.parent.scheduler.event.EventHandler;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;
import java.util.Map;


/**
 * @Author: limeng
 * @Date: 2019/8/20 16:44
 */
public class OracleImportJob extends ImportJobBase{
    private volatile static DBOptions dbOptions = null;

    public OracleImportJob() {
    }

    public OracleImportJob(SwapOptions opts) {
        super(opts);
    }



    private static DBOptions getDbOptions(){
        if(dbOptions == null){
            String name = DBOperationEnum.ORACLE_IMPORT.getName();
            Object options = getOptions(name);
            if(options != null){
                dbOptions = (DBOptions)options;
            }
        }
        return dbOptions;
    }

    /**
     * 保存
     * @param rows
     * @param schema
     * @param tableName
     * @return
     */
    private static void saveBySQL(PCollection<Map<String, ObjectCoder>> rows, Schema schema, String tableName){
        if(rows !=null && schema!=null){
            String insertSQL = getInsertSQL(schema, tableName);
            getLogger().info("insertSQL:{}",insertSQL);
            if(BaseUtil.isNotBlank(insertSQL)){
                rows.apply(ParDo.of(new TypeConversion.OracleAndMapType(schema)))
                        .apply(getOracleSwapImport().saveByIO(insertSQL));
            }
        }
    }

    /**
     * 批量保存sql
     * @param schema
     * @param tableName
     * @return
     */
    private static String getInsertSQL(Schema schema,String tableName){
        String result="";
        if(schema!=null && BaseUtil.isNotBlank(tableName)){
            result = JdbcUtil.getInsertSQL(schema,tableName);
        }
        return result;
    }

    private static Schema getSchema(){
        String[] dbColumn = getDbOptions().getDbColumn();
        String tableName = getDbOptions().getTableName();
        //表所有列
        Schema allSchema = getOracleSwapImport().desc(tableName);

        getLogger().info("mysql=>tableName:{}",tableName);
        return JdbcUtil.columnConversion(dbColumn, allSchema);
    }



    public static void save(PCollection<Map<String, ObjectCoder>> rows) {
        if(rows != null){
            String tableName = getDbOptions().getTableName();
            Schema schema = getSchema();

            if(schema == null){
                getLogger().info("schema is null");
            }


            saveBySQL(rows,schema,tableName);
        }
    }


    public static class OracleImportDispatcher implements EventHandler<OracleImportTaskEvent> {
        @Override
        public void handle(OracleImportTaskEvent event) {
            if(event.getType() == OracleImportType.T_IMPORT){
                getLogger().info("OracleImportDispatcher is start");

                if(CacheManager.isExist(DBOperationEnum.PCOLLECTION_QUERYS.getName())){
                    save((PCollection<Map<String, ObjectCoder>>)CacheManager.getCache(DBOperationEnum.PCOLLECTION_QUERYS.getName()));
                }

            }
        }
    }



}
