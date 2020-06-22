package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.event.MySQLExportType;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.process.pojo.DBOptions;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.swap.event.MySQLExportTaskEvent;
import com.knowlegene.parent.scheduler.event.EventHandler;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * @Author: limeng
 * @Date: 2019/9/9 14:57
 */
public class MySQLExportJob extends ExportJobBase {

    private static DBOptions dbOptions = null;

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
    public static PCollection<Row> queryBySQL(){
        String sql = getDbOptions().getDbSQL();
        String tableName = getDbOptions().getTableName();
        String[] dbColumn = JdbcUtil.getColumnBySqlRex(sql);

        Schema allSchema = getMySQLSwapExport().desc(tableName);

        getLogger().info("mysql=>sql:{},tableName:{}",sql,tableName);

        Schema schema = JdbcUtil.columnConversion(dbColumn, allSchema);

        JdbcIO.Read<Row> rows = getMySQLSwapExport().query(sql, schema);

        return getPipeline().apply(rows).setCoder(SchemaCoder.of(schema));
    }

    /**
     * 查询表
     * @return
     */
    protected static PCollection<Row> queryByTable(){

        Schema schema = getMysqlSchemas();
        String tableName = getDbOptions().getTableName();

        JdbcIO.Read<Row> rowRead = getMySQLSwapExport().queryByTable(tableName, schema);

        return getPipeline().apply(rowRead).setCoder(SchemaCoder.of(schema));

    }

    public static PCollection<Row> query(){
        String sql = getDbOptions().getDbSQL();
        PCollection<Row> rows=null;
        if(BaseUtil.isNotBlank(sql)){
            rows = queryBySQL();

        }else{
            rows = queryByTable();
        }
//        NestingFields nestingFields = options.getNestingFields();
//        if(nestingFields !=null && rows!=null){
//            return nestingFieldToEs(nestingFields,rows);
//        }
        return rows;
    }



    public static class MySQLExportDispatcher implements EventHandler<MySQLExportTaskEvent> {
        @Override
        public void handle(MySQLExportTaskEvent event) {
            if (event.getType() == MySQLExportType.T_EXPORT) {
                getLogger().info("HiveExportDispatcher is start");

                PCollection<Row> rows = query();
                CacheManager.setCache(DBOperationEnum.PCOLLECTION_QUERYS.getName(), rows);

            }
        }
    }

    //
//    /**
//     * 嵌套
//     * @param nestingFields
//     * @param querys
//     * @return
//     */
//    private PCollection<Row> nestingFieldToEs(NestingFields nestingFields, PCollection<Row> querys){
//        if(nestingFields == null){
//            return null;
//        }
//
//        String[] columns = nestingFields.getColumns();
//        //查询
//        String[] keys = nestingFields.getKeys();
//        KV<String, List<String>> nestings = nestingFields.mapToKV2();
//
//        Schema schema = getMysqlSchemas();
//        if(schema == null){
//            getLogger().error("schema is null");
//            return null;
//        }
//        if(columns == null){
//            List<String> strings = schema.getFieldNames();
//            strings.removeAll(nestings.getValue());
//            strings.removeAll(Arrays.asList(keys));
//            int size = strings.size();
//            if(!BaseUtil.isBlankSet(strings)){
//                nestingFields.setColumns(strings.toArray(new String[size]));
//            }
//        }
//
//        nestingFields.creatByNesting(nestings.getKey(),schema);
//        Schema resultSchema = nestingFields.getResultSchema();
//        if(resultSchema == null){
//            getLogger().error("resultSchema is null");
//            return null;
//        }
//
//        return  querys.apply(new ESTransform.NestingFieldTransform(Arrays.asList(keys),resultSchema,nestings));
//    }

}
