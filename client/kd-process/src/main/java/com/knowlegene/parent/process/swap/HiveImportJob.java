package com.knowlegene.parent.process.swap;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.constantenum.HiveTypeEnum;
import com.knowlegene.parent.config.common.event.HiveImportType;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.pojo.hive.HiveOptions;
import com.knowlegene.parent.process.swap.event.HiveImportTaskEvent;
import com.knowlegene.parent.process.transform.TypeConversion;
import com.knowlegene.parent.scheduler.event.EventHandler;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/8/20 16:50
 */
public class HiveImportJob extends ImportJobBase{

    protected static HiveOptions hiveOptions;

    public HiveImportJob() {

    }

    public HiveImportJob(SwapOptions opts) {
        super(opts);
    }

    private static HiveOptions getHiveOptions(){
        if(hiveOptions == null){
            String name = DBOperationEnum.HIVE_IMPORT.getName();
            Object options = getOptions(name);
            if(options != null){
                hiveOptions = (HiveOptions)options;
            }
        }
        return hiveOptions;
    }

    /**
     * 保存
     * @param rows
     * @param schema
     * @param tableName
     * @return
     */
    private static void saveBySQL(PCollection<Map<String, ObjectCoder>> rows, Schema schema, String tableName){
        if(rows !=null && schema!=null){
            String insertSQL = getInsertSQL(schema, tableName);
            getLogger().info("insertSQL:{}",insertSQL);
            if(BaseUtil.isNotBlank(insertSQL)){
                rows.apply(getHiveSwapImport().saveByIO(insertSQL));
            }
        }
    }

    /**
     * 批量保存sql
     * @param schema
     * @param tableName
     * @return
     */
    private static String getInsertSQL(Schema schema,String tableName){
       String result="";
       if(schema!=null && BaseUtil.isNotBlank(tableName)){
           String hivePartition = getHiveOptions().getHivePartition();
           result = JdbcUtil.getInsertSQL(schema,tableName,hivePartition);
       }
        return result;
    }




    /**
     * HCatalogIO保存
     * @param rows
     */
    private static boolean saveByHCatalog(PCollection<Map<String, ObjectCoder>> rows){
        if(rows != null){
            String uris = HiveTypeEnum.HCATALOGMETASTOREURIS.getName();
            String db=HiveTypeEnum.HIVEDATABASE.getName();
            String table=HiveTypeEnum.HIVETABLE.getName();
            Map<String, String> configProperties = new HashMap<>();
            String metastoreHostName = getHiveOptions().getHMetastoreHost();
            String metastorePort = getHiveOptions().getHMetastorePort();
            String hiveDatabase = getHiveOptions().getHiveDatabase();
            String hiveTableName = getHiveOptions().getHiveTableName();
            //hCatio参数
            if(BaseUtil.isBlank(metastoreHostName) || BaseUtil.isBlank(metastorePort) || BaseUtil.isBlank(hiveDatabase) || BaseUtil.isBlank(hiveTableName)){
                return false;
            }

            String uriValue = String.format("thrift://%s:%s",metastoreHostName,metastorePort);
            configProperties.put(uris,uriValue);
            configProperties.put(db,hiveDatabase);
            configProperties.put(table,hiveTableName);
            getLogger().info("HCatIO start=>hiveDB:{}.hiveTable:{}",hiveDatabase,hiveTableName);


            Schema schema = getHiveSchemas(false);
            if(schema == null){
                getLogger().error("schema is null");
                return false;
            }
            String hivePartition = getHiveOptions().getHivePartition();
            HashMap<String,String> partitionMap=null;
            if(BaseUtil.isNotBlank(hivePartition)){
                try {
                    partitionMap = JSON.parseObject(hivePartition, new TypeReference<HashMap<String,String>>() {});
                }catch (Exception e){
                    getLogger().error("hivePartition wrong format");
                    return false;
                }
            }
            PCollection<HCatRecord> hCatRecordPCollection = rows.apply(ParDo.of(new TypeConversion.MapObjectAndHCatRecord(schema)))
                    .setCoder(TypeConversion.getOutputCoder());


            hCatRecordPCollection.apply(Window.remerge()).apply(getHiveSwapImport().saveByHCatalogIO(configProperties,partitionMap));

            return true;
        }

        return false;

   }

    /**
     * 清空表
     * @param table
     * @return
     */
    private static int truncate(String table){
        int result=0;
        if(BaseUtil.isNotBlank(table)){
            String sql= "truncate table "+table;
            result = getHiveSwapImport().saveCommon(sql);
        }else{
            getLogger().error("table is null");
            result= -1;
        }
        return result;
    }

    private static Schema getHiveSchemas(boolean isTimeStr){
        String tableName = getHiveOptions().getHiveTableName();
        String[] dbColumn = getHiveOptions().getHiveColumn();
        Schema allSchema = getHiveSwapImport().descByTableName(tableName,isTimeStr);
        getLogger().info("hive=>tableName:{}",tableName);
        return JdbcUtil.columnConversion(dbColumn, allSchema);
    }


   public  static void save(PCollection<Map<String, ObjectCoder>> rows){
        if(rows ==null ){
           getLogger().info("rows is null");
           return;
        }

        boolean isHCatalog = saveByHCatalog(rows);
        if(isHCatalog) return;

        String tableName = getHiveOptions().getHiveTableName();
        boolean tableEmpty =getHiveOptions().getHiveTableEmpty()!=null?getHiveOptions().getHiveTableEmpty():false;
        Schema hiveSchema = getHiveSchemas(true);
        if(hiveSchema == null) return;
        getLogger().info("tableName:{}",tableName);

        if(tableEmpty){
            int truncate = truncate(tableName);
            if(truncate<0){
                return;
            }
        }
        saveBySQL(rows,hiveSchema,tableName);

   }



   public static class HiveImportDispatcher implements EventHandler<HiveImportTaskEvent>{
       @Override
       public void handle(HiveImportTaskEvent event) {
           if(event.getType() == HiveImportType.T_IMPORT){
               getLogger().info("HiveImportDispatcher is start");

               if(CacheManager.isExist(DBOperationEnum.PCOLLECTION_QUERYS.getName())){
                   PCollection<Map<String, ObjectCoder>>  rows = (PCollection<Map<String, ObjectCoder>>)CacheManager.getCache(DBOperationEnum.PCOLLECTION_QUERYS.getName());
                   save((PCollection<Map<String, ObjectCoder>>)CacheManager.getCache(DBOperationEnum.PCOLLECTION_QUERYS.getName()));
               }

           }
       }
   }


}
