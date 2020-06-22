package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.pojo.NestingFields;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.transform.ESTransform;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/10/11 18:19
 */
public class GbaseExportJob extends ExportJobBase {
    public GbaseExportJob() {
    }

    public GbaseExportJob(SwapOptions options) {
        super(options);
    }

    /**
     * 查询sql
     * @return
     */
    public PCollection<Row> queryBySQL(){
//        String sql = this.options.getDbSQL();
//        String tableName = options.getTableName();
//        Schema schema = getGbaseSchemas();
//        getLogger().info("gbase=>sql:{},tableName:{}",sql,tableName);
//        JdbcIO.Read<Row> rows = this.getGbaseSwap().query(sql, schema);
//        return super.getPipeline().apply(rows).setCoder(SchemaCoder.of(schema));
        return null;
    }


    /**
     * 查询表
     * @return
     */
    public PCollection<Row> queryByTable(){
//        Schema schema = getGbaseSchemas();
//        String tableName = options.getTableName();
//        getLogger().info("gbase=>tableName:{}",tableName);
//        JdbcIO.Read<Row> rowRead = this.getGbaseSwap().queryByTable(tableName, schema);
//        return super.getPipeline().apply(rowRead).setCoder(SchemaCoder.of(schema));
        return null;
    }

    /**
     * 嵌套
     * @param nestingFields
     * @param querys
     * @return
     */
    private PCollection<Row> nestingFieldToEs(NestingFields nestingFields,PCollection<Row> querys){
        if(nestingFields == null){
            return null;
        }

        String[] columns = nestingFields.getColumns();
        //查询
        String[] keys = nestingFields.getKeys();
        KV<String, List<String>> nestings = nestingFields.mapToKV2();

        Schema schema = getGbaseSchemas();
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





    public static PCollection<Row> query() {
//        String sql = this.options.getDbSQL();
//        PCollection<Row> rows=null;
//        if(BaseUtil.isNotBlank(sql)){
//            rows = queryBySQL();
//        }else {
//            rows = queryByTable();
//        }
//        NestingFields nestingFields = this.options.getNestingFields();
//        if(nestingFields !=null && rows!=null){
//            return nestingFieldToEs(nestingFields,rows);
//        }
//
//        return rows;
        return null;
    }
}
