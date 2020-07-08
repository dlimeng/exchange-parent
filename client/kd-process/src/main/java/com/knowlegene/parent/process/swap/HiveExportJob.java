package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.constantenum.HiveTypeEnum;
import com.knowlegene.parent.config.common.event.HiveExportType;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.pojo.hive.HiveOptions;
import com.knowlegene.parent.process.swap.event.HiveExportTaskEvent;
import com.knowlegene.parent.process.transform.TypeConversion;
import com.knowlegene.parent.scheduler.event.EventHandler;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * hive导出
 * @Author: limeng
 * @Date: 2019/8/20 16:50
 */
public class HiveExportJob extends ExportJobBase {

    protected static HiveOptions hiveOptions;

    public HiveExportJob() {
    }

    public HiveExportJob(SwapOptions options) {
        super(options);
    }

    private static HiveOptions getHiveOptions(){
        if(hiveOptions == null){
            String name = DBOperationEnum.HIVE_EXPORT.getName();
            Object options = getOptions(name);
            if(options != null){
                hiveOptions = (HiveOptions)options;
            }
        }
        return hiveOptions;
    }

    private static PCollection<Map<String, ObjectCoder>> getHCatRecordAndRow(PCollection<HCatRecord> ps) {
        if (ps != null) {
            String tableName = getHiveOptions().getHiveTableName();
            String[] dbColumn = getHiveOptions().getHiveColumn();
            //表所有列
            Schema allSchema = getHiveSwapExport().descByTableName(tableName,false);
            Schema schema = JdbcUtil.columnConversion(dbColumn, allSchema);
            if (schema != null) {
                return ps.apply(ParDo.of(new TypeConversion.HCatRecordAndMapObject(schema)));
            } else {
                getLogger().error("allSchema is null");
            }
        }
        return null;
    }


    /**
     * 查询sql
     * @return
     */
    private static PCollection<Map<String, ObjectCoder>> queryBySQL(){
        String hiveSQL = getHiveOptions().getHiveSQL();
        String tableName = getHiveOptions().getHiveTableName();

        String[] dbColumn = JdbcUtil.getColumnBySqlRex(hiveSQL);
        Schema allSchema = getHiveSwapExport().descByTableName(tableName, false);

        getLogger().info("hive=>sql:{},tableName:{}",hiveSQL,tableName);

        Schema schema = JdbcUtil.columnConversion(dbColumn, allSchema);
        if (schema == null) {
            getLogger().error("schema is null");
            return null;
        }

        return getPipeline().apply(getHiveSwapExport().query(hiveSQL))
                .apply(ParDo.of(new TypeConversion.MapObjectAndType(schema)));
    }
    /**
     * 查询表
     *
     * @return
     */
    private static PCollection<Map<String, ObjectCoder>> queryByTable() {
        String hiveSQL = getHiveOptions().getHiveSQL();
        if (BaseUtil.isNotBlank(hiveSQL)) {
           return  queryBySQL();
        } else {
            String tableName = getHiveOptions().getHiveTableName();
            String[] dbColumn = getHiveOptions().getHiveColumn();
            //表所有列
            Schema allSchema = getHiveSwapExport().descByTableName(tableName,false);
            Schema schema = JdbcUtil.columnConversion(dbColumn, allSchema);
            if (schema == null) {
                getLogger().error("schema is null");
                return null;
            }
            getLogger().info("hive=>tableName:{}", tableName);

            return getPipeline().apply(getHiveSwapExport().queryByTable(tableName))
                    .apply(ParDo.of(new TypeConversion.MapObjectAndType(schema)));
        }
    }

    /**
     * 查询
     */
    private static PCollection<HCatRecord> queryByHCatalog () {
        String uris = HiveTypeEnum.HCATALOGMETASTOREURIS.getName();
        String db = HiveTypeEnum.HIVEDATABASE.getName();
        String table = HiveTypeEnum.HIVETABLE.getName();
        Map<String, String> configProperties = new HashMap<>();
        String metastoreHostName = getHiveOptions().getHMetastoreHost();
        String metastorePort = getHiveOptions().getHMetastorePort();
        String hiveDatabase = getHiveOptions().getHiveDatabase();
        String hiveTableName = getHiveOptions().getHiveTableName();

        //hCatio参数
        if (BaseUtil.isBlank(metastoreHostName) || BaseUtil.isBlank(metastorePort) || BaseUtil.isBlank(hiveDatabase) || BaseUtil.isBlank(hiveTableName)) {
            return null;
        }


        String uriValue = String.format("thrift://%s:%s", metastoreHostName, metastorePort);
        configProperties.put(uris, uriValue);
        configProperties.put(db, hiveDatabase);
        configProperties.put(table, hiveTableName);
        getLogger().info("tableName:{}", hiveTableName);
        String hiveFilter = getHiveOptions().getHiveFilter();

        if (BaseUtil.isNotBlank(hiveFilter)) {
            return getPipeline().apply(getHiveSwapExport().queryByHCatalogIO(configProperties, hiveFilter));
        } else {
            return getPipeline().apply(getHiveSwapExport().queryByHCatalogIO(configProperties));
        }
    }


    public static PCollection<Map<String, ObjectCoder>> query () {
        PCollection<Map<String, ObjectCoder>> result = null;
        PCollection<HCatRecord> ps = queryByHCatalog();
        result = getHCatRecordAndRow(ps);

        if (result == null) result = queryByTable();

        return result;
    }


    public static class HiveExportDispatcher implements EventHandler<HiveExportTaskEvent> {
        @Override
        public void handle(HiveExportTaskEvent event) {
            if (event.getType() == HiveExportType.T_EXPORT) {
                getLogger().info("HiveExportDispatcher is start");
                PCollection<Map<String, ObjectCoder>> result = query();
                if(result != null){
                    CacheManager.setCache(DBOperationEnum.PCOLLECTION_QUERYS.getName(), result);
                }

            }
        }
    }


}



