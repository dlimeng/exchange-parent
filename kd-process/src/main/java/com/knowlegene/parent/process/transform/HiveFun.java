package com.knowlegene.parent.process.transform;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.config.util.PipelineSingletonUtil;
import com.knowlegene.parent.process.io.jdbc.HiveChange;
import com.knowlegene.parent.process.io.jdbc.impl.HiveChangeImpl;
import com.knowlegene.parent.process.model.NestingFields;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.*;

/**
 * 自定义函数
 * @Author: limeng
 * @Date: 2019/8/28 17:25
 */
public class HiveFun {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Resource
    private HiveChange hiveChange = new HiveChangeImpl();



    /**
     * 两个表根据字段合并
     * @param oldTable 表1
     * @param newTable 表2
     * @param mark 标识字段
     */
    public String mergeField(String oldTable,String newTable,String mark){
        //type
        //创建中间表
        KV<String, String> oldKV = getTableName(oldTable);
        KV<String, String> newKV = getTableName(newTable);
        if(oldKV == null || newKV == null || BaseUtil.isBlank(mark)){
            return "";
        }
        //创建合并表
        String tmpName=oldKV.getKey()+"."+oldKV.getValue()+"_"+newKV.getValue();
        String createSql="create if not exists table "+tmpName+" like "+oldTable;
        String truncateSql="truncate table "+tmpName;
        this.create(createSql);
        this.create(truncateSql);

        Pipeline pipeline = getPipeline();
        //老表差集
        String diffSql="select t1.* from"+ oldTable+" t1 LEFT JOIN "+newTable+" t2 on t1."+mark+" = t2."+mark+" where t2."+mark+" is null";
        Schema oldSchema = getSchema(oldTable);

        PCollection<HCatRecord> oldPc = pipeline.apply(hiveChange.listByIO(diffSql, oldSchema)).apply(ParDo.of(new TypeConversion.RowAndHCatRecord(oldSchema)));
        PCollection<HCatRecord> newPc = pipeline.apply(hiveChange.listByHCatIO(newKV.getValue(),newKV.getKey()));
        //新表老表合并
        PCollection<HCatRecord> merges = PCollectionList.of(oldPc).and(newPc).apply(Flatten.<HCatRecord>pCollections());


        merges.apply(hiveChange.saveByHCatIO((oldKV.getValue()+"_"+newKV.getValue()),oldKV.getKey()));

        return tmpName;
    }
    /**
     * 两个表根据字段合并
     * @param oldTable 表1
     * @param newTable 表2
     * @param mark 标识字段
     */
    public String mergeField2(String oldTable,String newTable,String mark){
        //type
        //创建中间表
        KV<String, String> oldKV = getTableName(oldTable);
        KV<String, String> newKV = getTableName(newTable);
        if(oldKV == null || newKV == null || BaseUtil.isBlank(mark)){
            return null;
        }

        //创建合并表
        String tmpName=oldKV.getKey()+"."+oldKV.getValue()+"_"+newKV.getValue();
        String createSql="create table if not exists  "+tmpName+" like "+oldTable;
        String truncateSql="truncate table "+tmpName;
        int createIndex = this.create(createSql);
        int truncateIndex = this.create(truncateSql);

        Pipeline pipeline = getPipeline();

        HCatSchema hCatSchema = hiveChange.descByHCatSchema(oldTable);
        if(hCatSchema == null){
            logger.error("hCatSchema is null");
            return null;
        }

        PCollection<HCatRecord> oldPc = pipeline.apply(hiveChange.listByHCatIO(oldKV.getValue(),oldKV.getKey()));
        PCollection<HCatRecord> newPc = pipeline.apply(hiveChange.listByHCatIO(newKV.getValue(),newKV.getKey()));
        //视图
        PCollectionView<Iterable<HCatRecord>> newView = newPc.apply(View.asIterable());
        //过滤出老版本数据加上修改完的数据
        PCollection<HCatRecord> f1=oldPc.apply(ParDo.of(new FilterTransform.FilterOldData(newView,hCatSchema,mark)).withSideInputs(newView));
        //合并
        PCollection<HCatRecord> mergeds = PCollectionList.of(f1).and(newPc).apply(Flatten.<HCatRecord>pCollections());
        //去重
        PCollection<HCatRecord> result = mergeds.apply(new DistinctTransform.DistinctHCatRecord(hCatSchema,mark));
        //保存
        result.apply(hiveChange.saveByHCatIO((oldKV.getValue()+"_"+newKV.getValue()),oldKV.getKey()));
        return tmpName;
    }

    private KV<String,String> getTableName(String table){
      if(BaseUtil.isNotBlank(table)){
          String[] split = table.split("\\.");
          if(split.length >1){
              return KV.of(split[0],split[1]);
          }else{
              return KV.of("default",split[0]);
          }
      }
      return null;
    }


    /**
     * 获取schema
     * @param table
     * @return
     */
    private Schema getSchema(String table){
        return hiveChange.descByTableName(table);
    }

    /**
     * 创建
     */
    public int create(String sql){
        return hiveChange.create(sql);
    }

    private Pipeline getPipeline(){
        return PipelineSingletonUtil.instance;
    }


    /**
     * 生成嵌套
     */
    public String nestingFieldToEs(NestingFields nestingFields){
       String result="";
       if(nestingFields == null){
           logger.error("nestings is null");
           return result;
       }
       if(nestingFields.isEmpty()){
           return result;
       }
        String tableName = nestingFields.getTableName();
        KV<String, List<String>> nestings = nestingFields.mapToKV2();
        String[] keys = nestingFields.getKeys();
        Schema schema = getSchema(tableName);
        String[] columns = nestingFields.getColumns();

        if(columns == null){
            List<String> strings = schema.getFieldNames();
            strings.removeAll(nestings.getValue());
            strings.removeAll(Arrays.asList(keys));
            int size = strings.size();
            if(!BaseUtil.isBlankSet(strings)){
                nestingFields.setColumns(strings.toArray(new String[size]));
            }
        }
        //查询
        if(nestings == null){
            logger.error("nestings is null");
            return result;
        }
        //嵌套表名，嵌套字段
        String nestingTable=tableName+"_to_es";
        String nestingName =nestings.getKey();

        String truncateSql="truncate table "+nestingTable;
        String createSql = nestingFields.creatByNesting(nestingTable,nestingName);
        //创建表
        int createIndex = this.create(createSql);
        int truncateIndex = this.create(truncateSql);

        //结果表
        Schema resultSchema = nestingFields.getResultSchema();
        String sql = nestingFields.nestingSelectSQL();
        Schema querySchema = nestingFields.getQuerySchema();
        if(querySchema == null || resultSchema==null){
            logger.error("schema is null");
            return result;
        }
        if(BaseUtil.isBlank(sql)){
            logger.error("select sql is null");
            return result;
        }
        sql+=" from "+tableName;
        Pipeline pipeline = getPipeline();
        String insertSQL = JdbcUtil.getInsertSQL(resultSchema, nestingTable);
        if(BaseUtil.isBlank(insertSQL)){
            logger.error("insert sql is null");
            return result;
        }
        //查询
        PCollection<Row> querys = pipeline.apply(hiveChange.queryByTable(sql))
                .apply(ParDo.of(new TypeConversion.MapObjectAndRow(querySchema))).setCoder(SchemaCoder.of(querySchema));

        //分组 keys
        //合并嵌套
        PCollection<Row> merages = querys.apply(ParDo.of(new FilterTransform.FilterByKeys(Arrays.asList(keys))))
                .apply(Combine.perKey(new CombineTransform.UniqueSets()))
                .apply(ParDo.of(new FilterTransform.FilterKeysAndJson(resultSchema, nestings)))
                .setCoder(SchemaCoder.of(resultSchema));

        //转存表
       merages.apply(hiveChange.batchHiveSave(insertSQL));
       result = nestingTable;
       return result;
    }







}
