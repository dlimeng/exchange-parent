package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.process.pojo.SwapOptions;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import static com.knowlegene.parent.config.util.JdbcUtil.getInsertSQL;

/**
 * @Author: limeng
 * @Date: 2019/8/20 16:44
 */
public class OracleImportJob extends ImportJobBase{
    public OracleImportJob() {
    }

    public OracleImportJob(SwapOptions opts) {
        super(opts);
    }

    /**
     * 查询sql
     * @return
     */
    public PCollection<Row> queryBySQL(){
//        String sql = this.options.getDbSQL();
//        String tableName = options.getTableName();
//        String[] dbColumn = JdbcUtil.getColumnBySqlRex(sql);
//        Schema allSchema = this.getOracleSwap().desc(tableName);
//        Schema schema = JdbcUtil.columnConversion(dbColumn, allSchema);
//        getLogger().info("query start=>tableName:{},sql:{}",tableName,sql);
//        JdbcIO.Read<Row> rows = this.getOracleSwap().query(sql, schema);
//        return super.getPipeline().apply(rows).setCoder(SchemaCoder.of(schema));
        return null;
    }


    /**
     * 查询表
     * @return
     */
    public PCollection<Row> queryByTable(){
//        String[] dbColumn = options.getDbColumn();
//        String tableName = options.getTableName();
//        //表所有列
//        Schema allSchema = this.getOracleSwap().desc(tableName);
//        Schema schema = JdbcUtil.columnConversion(dbColumn, allSchema);
//        JdbcIO.Read<Row> rowRead = this.getOracleSwap().queryByTable(tableName, schema);
//        getLogger().info("query start=>tableName:{}",tableName);
//        return super.getPipeline().apply(rowRead).setCoder(SchemaCoder.of(schema));
        return null;
    }


    public static PCollection<Row> query(){
//        String sql = this.options.getDbSQL();
//        if(BaseUtil.isNotBlank(sql)){
//            return this.queryBySQL();
//        }else{
//            return this.queryByTable();
//        }
        return null;
    }


    public static void save(PCollection<Row> rows){
//        Schema schema = getOracleSchemas();
//        if(schema != null){
//            String tableName = options.getTableName();
//            String insertSQL = getInsertSQL(schema, tableName);
//            getLogger().info("insertSQL:{}",insertSQL);
//            rows.setCoder(SchemaCoder.of(schema)).apply(this.getOracleSwap().saveByIO(insertSQL));
//        }else{
//            getLogger().error("schema is null");
//        }
    }
}
