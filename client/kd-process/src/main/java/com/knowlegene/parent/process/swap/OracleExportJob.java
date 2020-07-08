package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.event.OracleExportType;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.pojo.db.DBOptions;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.swap.event.OracleExportTaskEvent;
import com.knowlegene.parent.process.transform.TypeConversion;
import com.knowlegene.parent.scheduler.event.EventHandler;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/9/12 16:22
 */
public class OracleExportJob extends ExportJobBase {
    private static volatile DBOptions dbOptions = null;

    public OracleExportJob() {
    }

    public OracleExportJob(SwapOptions options) {
        super(options);
    }


    private static DBOptions getDbOptions(){
        if(dbOptions == null){
            String name = DBOperationEnum.ORACLE_EXPORT.getName();
            Object options = getOptions(name);
            if(options != null){
                dbOptions = (DBOptions)options;
            }
        }
        return dbOptions;
    }

    /**
     * 查询sql
     * @return
     */
    private static PCollection<Map<String, ObjectCoder>> queryBySQL(){
        String sql = getDbOptions().getDbSQL();
        String tableName = getDbOptions().getTableName();
        String[] dbColumn = JdbcUtil.getColumnBySqlRex(sql);
        Schema allSchema = getOracleSwapExport().desc(tableName);

        Schema schema = JdbcUtil.columnConversion(dbColumn, allSchema);
        getLogger().info("query start=>tableName:{},sql:{}",tableName,sql);

        return getPipeline().apply(getOracleSwapExport().query(sql))
                .apply(ParDo.of(new TypeConversion.MapObjectAndType(schema)));
    }


    /**
     * 查询表
     * @return
     */
    private static PCollection<Map<String, ObjectCoder>> queryByTable(){
        String[] dbColumn =getDbOptions().getDbColumn();
        String tableName = getDbOptions().getTableName();
        //表所有列
        Schema allSchema = getOracleSwapExport().desc(tableName);
        Schema schema = JdbcUtil.columnConversion(dbColumn, allSchema);

        getLogger().info("query start=>tableName:{}",tableName);

        return getPipeline().apply(getOracleSwapExport().queryByTable(tableName))
                .apply(ParDo.of(new TypeConversion.MapObjectAndType(schema)));
    }


    public static PCollection<Map<String, ObjectCoder>> query(){
        String sql = getDbOptions().getDbSQL();
        if(BaseUtil.isNotBlank(sql)){
            return queryBySQL();
        }else{
            return queryByTable();
        }
    }


    public static class OracleExportDispatcher implements EventHandler<OracleExportTaskEvent> {
        @Override
        public void handle(OracleExportTaskEvent event) {
            if (event.getType() == OracleExportType.T_EXPORT) {
                getLogger().info("OracleExportDispatcher is start");
                PCollection<Map<String, ObjectCoder>> result = query();
                if(result != null){
                    CacheManager.setCache(DBOperationEnum.PCOLLECTION_QUERYS.getName(), result);
                }
            }
        }
    }

}
