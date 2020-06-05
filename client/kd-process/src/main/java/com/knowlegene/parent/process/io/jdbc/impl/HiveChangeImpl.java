package com.knowlegene.parent.process.io.jdbc.impl;

import com.knowlegene.parent.config.common.constantenum.HiveTypeEnum;
import com.knowlegene.parent.config.dao.impl.BaseDaoImpl;
import com.knowlegene.parent.config.pojo.sql.SQLResult;
import com.knowlegene.parent.config.util.*;
import com.knowlegene.parent.process.common.constantenum.DataSourceEnum;
import com.knowlegene.parent.process.io.jdbc.HiveChange;
import com.knowlegene.parent.process.model.ObjectCoder;
import com.knowlegene.parent.process.transform.HiveDb;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 原子业务功能
 * @Author: limeng
 * @Date: 2019/7/22 19:00
 */
public class HiveChangeImpl extends BaseDaoImpl implements HiveChange {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Resource(name = "hiveDataSource")
    private DataSource hiveDataSource;
    @Resource(name = "hiveMetastore")
    private Map<String, String> hiveMetastore;
    /**
     * 创建表
     * @param sql sql
     * @return
     */
    @Override
    public int create(String sql) {
        return this.getHiveDao().createTable(sql);
    }

    /**
     * 查询
     * @param sql sql
     * @param type 字段类型
     */
    @Override
    public void list(String sql, Schema type) {
        this.getHiveDao().list(sql,type);
    }

    /**
     * 查询
     * @param sql sql
     * @param type 字段类型
     * @return
     */
    @Override
    public JdbcIO.Read<Row> listByIO(String sql, Schema type) {
        return this.getHiveDao().listByIO(sql,type);
    }

    @Override
    public HCatalogIO.Read listByHCatIO(String table, String dbName) {
        if(BaseUtil.isBlank(table) || BaseUtil.isBlank(dbName)){
            return null;
        }
        String uris = HiveTypeEnum.HCATALOGMETASTOREURIS.getName();
        Map<String, String> configProperties = new HashMap<>();
        configProperties.put(uris,hiveMetastore.get(uris));
        logger.info("table:{},dbName:{}",table,dbName);
        return HCatalogIO.read().withConfigProperties(configProperties).withDatabase(dbName).withTable(table);
    }


    /**
     * 详情
     * @param sql sql
     * @return 结果
     */
    @Override
    public Schema desc(String sql) {
        return this.getHiveDao().desc(sql);
    }

    /**
     * 详情
     * @param tableName 表名
     * @return r
     */
    @Override
    public Schema descByTableName(String tableName) {
        return this.getHiveDao().descByTableName(tableName);
    }

    @Override
    public HCatSchema descByHCatSchema(String tableName) {
        if(BaseUtil.isBlank(tableName)){
            return null;
        }
        return this.getHiveDao().descByTableNameAndHCat(tableName);
    }

    /**
     * 保存
     * @param sql sql
     * @param row row
     */
    @Override
    public void save(String sql, Row row) {
        this.getHiveDao().save(sql,row);
    }



    /**
     * 保存jdbcio
     * @param sql sql
     * @return
     */
    @Override
    public JdbcIO.Write<Row> saveByIO(String sql) {
        return this.getHiveDao().saveByIO(sql);
    }

    /**
     * 删除
     * @param sql sql
     * @return
     */
    @Override
    public int delete(String sql) {
        return this.getHiveDao().delete(sql);
    }


    /**
     * 分配链路
     * @param pCollections
     */
    @Override
    public PCollection<String> distributionLink(PCollection<String> pCollections) {
        logger.info("distributionLink文件的链路分配操作，ddl,save");
        PCollection<String> ddlCollection = pCollections.apply(ParDo.of(new HiveDb.DoFnHiveDdlLink<String>()));
        return ddlCollection.apply(ParDo.of(new HiveDb.DoFnHiveSaveLink<String>()));
    }

    /**
     * 无序
     * @param sql
     * @return
     */
    @Override
    public PCollection<String> ddl(PCollection<String> sql) {
        return sql.apply(ParDo.of(new HiveDb.DoFnHiveDdlLink<String>()));
    }
    /**
     * 保存
     * @param sql
     * @return
     */
    @Override
    public PCollection<String> save(PCollection<String> sql) {
        return sql.apply(ParDo.of(new HiveDb.DoFnHiveSaveLink<String>()));
    }

    @Override
    public PCollection<String> save(PCollection<Row> sql, Schema type) {

        return null;
    }

    /**
     * 保存
     * @param sql
     * @param isOrder
     * @return
     */
    @Override
    public List<String> saveByOrder(List<String> sql, boolean isOrder) {
        List<String> result= null;
        if(isOrder){
            result = this.orderLinkSave(sql);
        }else{
            PCollection<String> dbRowCollection =getPipeline()
                    .apply(Create.of(sql)).setCoder(StringUtf8Coder.of());
            this.save(dbRowCollection);
        }
        return result;
    }
    /**
     * 定义
     * @param sql
     * @param isOrder
     * @return
     */
    @Override
    public List<String> ddlByOrder(List<String> sql, boolean isOrder) {
        List<String> result= null;
        if(isOrder){
            result = this.orderLinkDdl(sql);
        }else{
            PCollection<String> dbRowCollection =getPipeline()
                    .apply(Create.of(sql)).setCoder(StringUtf8Coder.of());
            this.ddl(dbRowCollection);
        }
        return result;
    }
    /**
     * 保存
     * @param
     * @param
     * @return
     */
    @Override
    public void saveByOrder(SQLResult resultDO) {
        if(resultDO != null){
            Boolean isOrder = resultDO.getIsOrder();
            List<String> inserts = resultDO.getInserts();
            if(isOrder == null ){
                this.saveByOrder(inserts,true);
            }else{
                this.saveByOrder(inserts,isOrder);
            }
        }
    }


    @Override
    public HCatalogIO.Write saveByHCatIO(String table,String dbName){
        if(BaseUtil.isNotBlank(table) && BaseUtil.isNotBlank(dbName)){
            String uris = HiveTypeEnum.HCATALOGMETASTOREURIS.getName();
            Map<String, String> configProperties = new HashMap<>();
            configProperties.put(uris,hiveMetastore.get(uris));
            logger.info("table:{},dbName:{}",table,dbName);
            return HCatalogIO.write().withConfigProperties(configProperties).withDatabase(dbName).withTable(table).withBatchSize(1024L);
        }
        return null;
    }

    @Override
    public JdbcIO.Read<Map<String, ObjectCoder>> queryByTable(String sql) {
        if(BaseUtil.isNotBlank(sql)){
            try {
                return HiveDb.selectByHive(hiveDataSource,sql);
            } catch (Exception e) {
                logger.error("query is error");
            }
        }
        return null;
    }

    @Override
    public JdbcIO.Write<Row> batchHiveSave(String sql) {
        if(BaseUtil.isNotBlank(sql)){
            return HiveDb.batchHiveSave(hiveDataSource,sql);
        }
        return null;
    }

    /**
     * 定义
     * @param
     * @param
     * @return
     */
    @Override
    public void ddlByOrder(SQLResult resultDO) {
        if(resultDO != null){
            Boolean isOrder = resultDO.getIsOrder();
            List<String> inserts = resultDO.getDdls();
            if(isOrder == null ){
                this.ddlByOrder(inserts,true);
            }else{
                this.ddlByOrder(inserts,isOrder);
            }
        }

    }



    private List<String> orderLinkSave(List<String> sqls){
        List<String> result= null;
        if(!BaseUtil.isBlankSet(sqls)){
            result = new ArrayList<>();
            HiveTypeEnum save1 = HiveTypeEnum.SAVE1;
            HiveTypeEnum load1 = HiveTypeEnum.LOAD1;
            HiveTypeEnum select1 = HiveTypeEnum.SELECT1;
            HiveTypeEnum set1 = HiveTypeEnum.SET1;
            int value = DataSourceEnum.HIVE.getValue();
            KV<Schema, Row> initWrite = JdbcUtil.getInitWrite();
            HiveTypeEnum create1 = HiveTypeEnum.CREATE1;
            Pipeline pipeline = PipelineSingletonUtil.instance;
            PipelineOptions options = pipeline.getOptions();
            Pipeline pipeline1 = Pipeline.create(options);
            PCollection<Row> rowCollection = pipeline1.apply(Create.of(initWrite.getValue()));
            String name="orderLinkSave";
            List<String> variables = HSqlThreadLocalUtil.getJob();

            //注册链路
            for (int i=0;i<sqls.size();i++){
                String sql= sqls.get(i);
                int status = BaseSqlParserFactoryUtil.generateParser(value, sql);
                if(status == save1.getValue() || status == load1.getValue()){
                    logger.info("ordered operation save=>sql:{}",sql);
                    rowCollection.apply(name+i,this.getHiveDao().saveByVariable(sql,variables)).getPipeline().run();
                }else if(status !=  select1.getValue() && status != set1.getValue()){
                    logger.info("ordered operation save=>sql:{}",sql);
                    rowCollection.apply(name+i,saveByIO(sql)).getPipeline().run();
                }else{
                    result.add(sql);
                }
            }
        }
        return result;
    }

    private List<String> orderLinkDdl(List<String> sqls){
        List<String> result= null;
        if(!BaseUtil.isBlankSet(sqls)){
            result = new ArrayList<>();
            HiveTypeEnum create1 = HiveTypeEnum.CREATE1;
            HiveTypeEnum drop1 = HiveTypeEnum.DROP1;
            HiveTypeEnum set1 = HiveTypeEnum.SET1;
            int value = DataSourceEnum.HIVE.getValue();
            boolean ddlStatus=false;
            HiveTypeEnum save1 = HiveTypeEnum.SAVE1;
            HiveTypeEnum load1 = HiveTypeEnum.LOAD1;
            HiveTypeEnum select1 = HiveTypeEnum.SELECT1;
            //注册链路
            for (int i=0;i<sqls.size();i++){
                String sql= sqls.get(i);
                int status = BaseSqlParserFactoryUtil.generateParser(value, sql);
                ddlStatus = (status != select1.getValue() );
                if(ddlStatus){
                    int index = this.create(sql);
                    logger.info("ordered operation ddl=>sql:{},rowsAffected:{}",sql,index);
                }else{
                    result.add(sql);
                }
            }
        }
        return result;
    }



    private  Pipeline getPipeline(){
        return  PipelineSingletonUtil.instance;
    }
}
