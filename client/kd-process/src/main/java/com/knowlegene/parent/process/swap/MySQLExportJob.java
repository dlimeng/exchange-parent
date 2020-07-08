package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.event.MySQLExportType;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.pojo.db.DBOptions;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.swap.event.MySQLExportTaskEvent;
import com.knowlegene.parent.process.transform.TypeConversion;
import com.knowlegene.parent.scheduler.event.EventHandler;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/9/9 14:57
 */
public class MySQLExportJob extends ExportJobBase {

    private volatile static DBOptions dbOptions = null;

    public MySQLExportJob() {
    }

    public MySQLExportJob(SwapOptions options) {
        super(options);
    }

    public static DBOptions getDbOptions(){
        if(dbOptions == null){
            String name = DBOperationEnum.MYSQL_EXPORT.getName();
            Object options = getOptions(name);
            if(options != null){
                dbOptions = (DBOptions)options;
            }
        }
        return dbOptions;
    }



    private static Schema getMysqlSchemas(){
        String tableName = getDbOptions().getTableName();
        //表所有列
        Schema allSchema = getMySQLSwapExport().desc(tableName);
        getLogger().info("mysql=>tableName:{}",tableName);
        return allSchema;

    }

    /**
     * 查询sql
     * @return
     */
    private static PCollection<Map<String, ObjectCoder>> queryBySQL(){
        String sql = getDbOptions().getDbSQL();
        String tableName = getDbOptions().getTableName();
        String[] dbColumn = JdbcUtil.getColumnBySqlRex(sql);

        Schema allSchema = getMySQLSwapExport().desc(tableName);

        getLogger().info("mysql=>sql:{},tableName:{}",sql,tableName);

        Schema schema = JdbcUtil.columnConversion(dbColumn, allSchema);



        return getPipeline().apply(getMySQLSwapExport().query(sql))
                .apply(ParDo.of(new TypeConversion.MapObjectAndType(schema)));
    }

    /**
     * 查询表
     * @return
     */
    private static PCollection<Map<String, ObjectCoder>> queryByTable(){

        Schema schema = getMysqlSchemas();
        String tableName = getDbOptions().getTableName();

        return getPipeline().apply(getMySQLSwapExport().queryByTable(tableName))
                .apply(ParDo.of(new TypeConversion.MapObjectAndType(schema)));

    }

    public static PCollection<Map<String, ObjectCoder>> query(){
        String sql = getDbOptions().getDbSQL();
        PCollection<Map<String, ObjectCoder>> rows=null;
        if(BaseUtil.isNotBlank(sql)){
            rows = queryBySQL();

        }else{
            rows = queryByTable();
        }

        return rows;
    }



    public static class MySQLExportDispatcher implements EventHandler<MySQLExportTaskEvent> {
        @Override
        public void handle(MySQLExportTaskEvent event) {
            if (event.getType() == MySQLExportType.T_EXPORT) {
                getLogger().info("MySQLExportDispatcher is start");
                PCollection<Map<String, ObjectCoder>> result = query();
                if(result != null){
                    CacheManager.setCache(DBOperationEnum.PCOLLECTION_QUERYS.getName(), result);
                }
            }
        }
    }



}
