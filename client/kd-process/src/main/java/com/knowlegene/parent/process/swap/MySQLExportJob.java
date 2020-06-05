package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.process.model.SwapOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * @Author: limeng
 * @Date: 2019/9/9 14:57
 */
public class MySQLExportJob extends ExportJobBase {
    public MySQLExportJob() {
    }

    public MySQLExportJob(SwapOptions options) {
        super(options);
    }

    /**
     * 保存
     * @param rows
     * @param schema
     * @param tableName
     * @return
     */
    private void saveBySQL(PCollection<Row> rows, Schema schema, String tableName){
        if(rows !=null && schema!=null){
            String insertSQL = this.getInsertSQL(schema, tableName);
            getLogger().info("insertSQL:{}",insertSQL);
            if(BaseUtil.isNotBlank(insertSQL)){
                rows.apply(this.getMySQLSwap().saveByIO(insertSQL));
            }
        }
    }

    /**
     * 批量保存sql
     * @param schema
     * @param tableName
     * @return
     */
    private String getInsertSQL(Schema schema,String tableName){
        String result="";
        if(schema!=null && BaseUtil.isNotBlank(tableName)){
            result = JdbcUtil.getInsertSQL(schema,tableName);
        }
        return result;
    }

    @Override
    public void save(PCollection<Row> rows) {
        if(rows != null){
            String tableName = options.getTableName();
            Schema schema = getMysqlSchemas();
            if(schema == null){
                getLogger().info("schema is null");
            }
            PCollection<Row> newRows = rows.setCoder(SchemaCoder.of(schema));
            saveBySQL(newRows,schema,tableName);
        }
    }
}
