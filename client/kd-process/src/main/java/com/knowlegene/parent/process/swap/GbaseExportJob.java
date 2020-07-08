package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.event.GbaseExportType;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.pojo.db.DBOptions;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.swap.event.GbaseExportTaskEvent;
import com.knowlegene.parent.process.transform.TypeConversion;
import com.knowlegene.parent.scheduler.event.EventHandler;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/10/11 18:19
 */
public class GbaseExportJob extends ExportJobBase {
    private volatile static DBOptions dbOptions = null;

    public GbaseExportJob() {
    }

    public GbaseExportJob(SwapOptions options) {
        super(options);
    }

    public static DBOptions getDbOptions(){
        if(dbOptions == null){
            String name = DBOperationEnum.GBASE_EXPORT.getName();
            Object options = getOptions(name);
            if(options != null){
                dbOptions = (DBOptions)options;
            }
        }
        return dbOptions;
    }


    private static Schema getGbaseSchemas(){
        String[] dbColumn = getDbOptions().getDbColumn();
        String tableName = getDbOptions().getTableName();
        //表所有列
        Schema allSchema = getGbaseSwapExport().desc(tableName);

        getLogger().info("gbase=>tableName:{}",tableName);
        return JdbcUtil.columnConversion(dbColumn, allSchema);

    }


    /**
     * 查询sql
     * @return
     */
    private static PCollection<Map<String, ObjectCoder>> queryBySQL(){
        String sql = getDbOptions().getDbSQL();
        String tableName = getDbOptions().getTableName();
        Schema schema = getGbaseSchemas();
        getLogger().info("gbase=>sql:{},tableName:{}",sql,tableName);

        return getPipeline().apply(getGbaseSwapExport().query(sql))
                .apply(ParDo.of(new TypeConversion.MapObjectAndType(schema)));
    }


    /**
     * 查询表
     * @return
     */
    private static PCollection<Map<String, ObjectCoder>> queryByTable(){
        Schema schema = getGbaseSchemas();
        String tableName = getDbOptions().getTableName();
        getLogger().info("gbase=>tableName:{}",tableName);

        return getPipeline().apply(getGbaseSwapExport().queryByTable(tableName))
                .apply(ParDo.of(new TypeConversion.MapObjectAndType(schema)));

    }


    public static PCollection<Map<String, ObjectCoder>> query() {
        String sql = getDbOptions().getDbSQL();
        PCollection<Map<String, ObjectCoder>> rows=null;
        if(BaseUtil.isNotBlank(sql)){
            rows = queryBySQL();
        }else {
            rows = queryByTable();
        }


        return rows;

    }


    public static class GbaseExportDispatcher implements EventHandler<GbaseExportTaskEvent> {
        @Override
        public void handle(GbaseExportTaskEvent event) {
            if (event.getType() == GbaseExportType.T_EXPORT) {
                getLogger().info("GbaseExportDispatcher is start");
                PCollection<Map<String, ObjectCoder>> result = query();
                if(result != null){
                    CacheManager.setCache(DBOperationEnum.PCOLLECTION_QUERYS.getName(), result);
                }

            }
        }
    }
}
