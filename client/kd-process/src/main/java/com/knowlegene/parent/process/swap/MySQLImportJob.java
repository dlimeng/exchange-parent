package com.knowlegene.parent.process.swap;


import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.process.model.NestingFields;
import com.knowlegene.parent.process.model.SwapOptions;
import com.knowlegene.parent.process.transform.ESTransform;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

import java.util.Arrays;
import java.util.List;

import static org.apache.beam.sdk.transforms.GroupIntoBatches.ofSize;


/**
 * @Author: limeng
 * @Date: 2019/8/20 16:40
 */
public class MySQLImportJob extends ImportJobBase{

    public MySQLImportJob() {
    }

    public MySQLImportJob(SwapOptions opts) {
        super(opts);
    }


    /**
     * 查询sql
     * @return
     */
    public PCollection<Row> queryBySQL(){
        String sql = this.options.getDbSQL();
        String tableName = options.getTableName();
        String[] dbColumn = JdbcUtil.getColumnBySqlRex(sql);
        Schema allSchema = this.getMySQLSwap().desc(tableName);

        getLogger().info("mysql=>sql:{},tableName:{}",sql,tableName);

        Schema schema = JdbcUtil.columnConversion(dbColumn, allSchema);
        JdbcIO.Read<Row> rows = this.getMySQLSwap().query(sql, schema);
        return super.getPipeline().apply(rows).setCoder(SchemaCoder.of(schema));
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

    /**
     * 查询表
     * @return
     */
    public PCollection<Row> queryByTable(){
        Schema schema = getMysqlSchemas();
        String tableName = options.getTableName();
        JdbcIO.Read<Row> rowRead = this.getMySQLSwap().queryByTable(tableName, schema);
        return super.getPipeline().apply(rowRead).setCoder(SchemaCoder.of(schema));
    }


    /**
     * 嵌套
     * @param nestingFields
     * @param querys
     * @return
     */
    private PCollection<Row> nestingFieldToEs(NestingFields nestingFields, PCollection<Row> querys){
        if(nestingFields == null){
            return null;
        }

        String[] columns = nestingFields.getColumns();
        //查询
        String[] keys = nestingFields.getKeys();
        KV<String, List<String>> nestings = nestingFields.mapToKV2();

        Schema schema = getMysqlSchemas();
        if(schema == null){
            getLogger().error("schema is null");
            return null;
        }
        if(columns == null){
            List<String> strings = schema.getFieldNames();
            strings.removeAll(nestings.getValue());
            strings.removeAll(Arrays.asList(keys));
            int size = strings.size();
            if(!BaseUtil.isBlankSet(strings)){
                nestingFields.setColumns(strings.toArray(new String[size]));
            }
        }

        nestingFields.creatByNesting(nestings.getKey(),schema);
        Schema resultSchema = nestingFields.getResultSchema();
        if(resultSchema == null){
            getLogger().error("resultSchema is null");
            return null;
        }

        return  querys.apply(new ESTransform.NestingFieldTransform(Arrays.asList(keys),resultSchema,nestings));
    }

    @Override
    public PCollection<Row> query(){
        String sql = this.options.getDbSQL();
        PCollection<Row> rows=null;
        if(BaseUtil.isNotBlank(sql)){
            rows = this.queryBySQL();

        }else{
            rows = this.queryByTable();
        }
        NestingFields nestingFields = this.options.getNestingFields();
        if(nestingFields !=null && rows!=null){
            return nestingFieldToEs(nestingFields,rows);
        }

        return rows;
    }

    @Override
    public void save(PCollection<Row> rows){
        Schema schema = getMysqlSchemas();
        if(schema != null){
            String tableName = options.getTableName();
            String insertSQL = getInsertSQL(schema, tableName);
            getLogger().info("insertSQL:{}",insertSQL);
            PDone pdone = rows.setCoder(SchemaCoder.of(schema)).apply(this.getMySQLSwap().saveByIO(insertSQL));
        }else{
            getLogger().error("schema is null");
        }
    }
}
