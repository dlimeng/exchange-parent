package com.knowlegene.parent.process.swap;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.knowlegene.parent.config.common.constantenum.HiveTypeEnum;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.process.model.SwapOptions;
import com.knowlegene.parent.process.transform.TypeConversion;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.awt.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/8/20 16:50
 */
public class HiveImportJob extends ImportJobBase{



    public HiveImportJob() {

    }

    public HiveImportJob(SwapOptions opts) {
        super(opts);
    }

    /**
     * 保存
     * @param rows
     * @param schema
     * @param tableName
     * @return
     */
    private void saveBySQL(PCollection<Row> rows,Schema schema,String tableName){
        if(rows !=null && schema!=null){
            String insertSQL = this.getInsertSQL(schema, tableName);
            getLogger().info("insertSQL:{}",insertSQL);
            if(BaseUtil.isNotBlank(insertSQL)){
                rows.apply(this.getHiveSwap().saveByIO(insertSQL));
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



    /**
     * 匹配hive列
     * @return
     */
    private boolean matchColumn(){
       String[] hiveColumn = options.getHiveColumn();
       String[] dbColumn = options.getDbColumn();
       boolean result=false;
       if(dbColumn == null && hiveColumn==null){
           result =true;
       }
       if(dbColumn !=null && hiveColumn!=null){
           if(dbColumn.length == hiveColumn.length){
               result = true;
           }
       }
       return result;
   }

    /**
     * HCatalogIO保存
     * @param rows
     */
    private void saveByHCatalog(PCollection<Row> rows){
        if(rows != null){
            String uris = HiveTypeEnum.HCATALOGMETASTOREURIS.getName();
            String db=HiveTypeEnum.HIVEDATABASE.getName();
            String table=HiveTypeEnum.HIVETABLE.getName();
            Map<String, String> configProperties = new HashMap<>();
            String metastoreHostName = options.getHMetastoreHost();
            String metastorePort = options.getHMetastorePort();
            String hiveDatabase = options.getHiveDatabase();
            String hiveTableName = options.getHiveTableName();
            //hCatio参数
            if(BaseUtil.isBlank(metastoreHostName) || BaseUtil.isBlank(metastorePort) || BaseUtil.isBlank(hiveDatabase) || BaseUtil.isBlank(hiveTableName)){
                return;
            }

            String uriValue = String.format("thrift://%s:%s",metastoreHostName,metastorePort);
            configProperties.put(uris,uriValue);
            configProperties.put(db,hiveDatabase);
            configProperties.put(table,hiveTableName);
            getLogger().info("HCatIO start=>hiveDB:{}.hiveTable:{}",hiveDatabase,hiveTableName);


            Schema schema = getHiveSchemas(false);
            if(schema == null){
                getLogger().error("schema is null");
                return;
            }
            String hivePartition = options.getHivePartition();
            HashMap<String,String> partitionMap=null;
            if(BaseUtil.isNotBlank(hivePartition)){
                try {
                    partitionMap = JSON.parseObject(hivePartition, new TypeReference<HashMap<String,String>>() {});
                }catch (Exception e){
                    getLogger().error("hivePartition wrong format");
                    return;
                }
            }
            PCollection<HCatRecord> hCatRecordPCollection = rows.apply(ParDo.of(new TypeConversion.RowAndHCatRecord(schema)))
                    .setCoder(TypeConversion.getOutputCoder());


            hCatRecordPCollection.apply(Window.remerge()).apply(this.getHiveSwap().saveByHCatalogIO(configProperties,partitionMap));

        }

   }

    /**
     * 清空表
     * @param table
     * @return
     */
    private int truncate(String table){
        int result=0;
        if(BaseUtil.isNotBlank(table)){
            String sql= "truncate table "+table;
            result = this.getHiveSwap().saveCommon(sql);
        }else{
            getLogger().error("table is null");
            result= -1;
        }
        return result;
    }



   @Override
   public void save(PCollection<Row> rows){
        if(rows ==null ){
           getLogger().info("rows is null");
           return;
        }
        if(matchColumn()){
            if(isHCatalogIOStatus()){
                saveByHCatalog(rows);
            }else{
                String tableName = options.getHiveTableName();
                boolean tableEmpty=options.getHiveTableEmpty()!=null?options.getHiveTableEmpty():false;
                Schema hiveSchema = getHiveSchemas(true);
                if(hiveSchema == null) return;
                getLogger().info("tableName:{}",tableName);

                if(tableEmpty){
                    int truncate = truncate(tableName);
                    if(truncate<0){
                        return;
                    }
                }
                PCollection<Row> newRow = rows.setCoder(SchemaCoder.of(hiveSchema));
                saveBySQL(newRow,hiveSchema,tableName);
            }
        }else{
            getLogger().error("Columns is null");
        }
   }



}
