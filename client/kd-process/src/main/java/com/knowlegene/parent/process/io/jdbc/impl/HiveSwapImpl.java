package com.knowlegene.parent.process.io.jdbc.impl;

import com.knowlegene.parent.config.common.constantenum.HiveTypeEnum;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.io.jdbc.AbstractSwapBase;
import com.knowlegene.parent.process.io.jdbc.HiveSwap;
import com.knowlegene.parent.process.model.ObjectCoder;
import com.knowlegene.parent.process.transform.TypeConversion;
import com.knowlegene.parent.process.util.HiveDataSourceUtil;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/8/22 14:46
 */
public class HiveSwapImpl extends AbstractSwapBase implements HiveSwap {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public DataSource getDataSource() {
        DataSource dataSource = HiveDataSourceUtil.getDataSource();
        if(dataSource == null){
            logger.error("hive dataSource is null");
        }
        return dataSource;
    }

    /**
     * 获取Schema
     * @param tableName 表名
     * @return
     */
    @Override
    public Schema descByTableName(String tableName,boolean isTimeStr) {
        try {
            return this.getHiveSchema(tableName,isTimeStr);
        }catch (Exception e){
            logger.error("desc=>tableName:{},msg:{}",tableName,e.getMessage());
        }
        return null;
    }

    /**
     * 带批次的sql row 保存
     * @param sql
     * @return
     */
    @Override
    public JdbcIO.Write<Row> saveByIO(String sql) {
        if(BaseUtil.isBlank(sql)){
            logger.error("sql is  null");
            return null;
        }
        return this.batchHiveSave(sql);
    }

    /**
     * 通用保存操作
     * @param sql
     * @return
     */
    @Override
    public int saveCommon(String sql) {
        int result = 0;
        if(BaseUtil.isBlank(sql)){
            result = -1;
            logger.error("sql is null");
        }else{
            try {
                result = this.write(sql);
            } catch (SQLException e) {
                result = -1;
                logger.error("sql:{}",sql);
            }
        }
        return result;
    }

    /**
     * 类型转换 rows转换HCatRecord
     * @param rows
     * @return
     */
    @Override
    public PCollection<HCatRecord> setHCatRecordAndRow(PCollection<Row> rows) {
        if(rows != null){
            Schema schema = rows.getSchema();
            return rows.apply(ParDo.of(new TypeConversion.RowAndHCatRecord(schema)));
        }
        return null;
    }

    /**
     * HCatalogIO保存
     * @param ops
     * @return
     */
    @Override
    public HCatalogIO.Write saveByHCatalogIO(Map<String, String> ops,Map<String, String> partition) {
        if(!BaseUtil.isBlankMap(ops)){
            String uris = HiveTypeEnum.HCATALOGMETASTOREURIS.getName();
            String db=HiveTypeEnum.HIVEDATABASE.getName();
            String table=HiveTypeEnum.HIVETABLE.getName();
            Map<String, String> configProperties = new HashMap<>();
            configProperties.put(uris,ops.get(uris));
            configProperties.put("hive.merge.mapfiles","true");
            configProperties.put("hive.merge.mapredfiles","true");
            configProperties.put("hive.merge.smallfiles.avgsize","1024000000");
            configProperties.put("mapred.max.split.size","256000000");

            configProperties.put("mapred.min.split.size.per.node","192000000");
            configProperties.put("mapred.min.split.size.per.rack","192000000");
            configProperties.put("hive.input.format","org.apache.hadoop.hive.ql.io.CombineHiveInputFormat");

            HCatalogIO.Write write = HCatalogIO.write()
                    .withConfigProperties(configProperties)
                    .withDatabase(ops.get(db))
                    .withTable(ops.get(table))
                    .withBatchSize(1024L);

            if(!BaseUtil.isBlankMap(partition)){
                write.withPartition(partition);
            }
            return write;
        }
        return null;
    }


    /**
     * HCatalogIO 查询
     * @param ops
     * @return
     */
    @Override
    public HCatalogIO.Read queryByHCatalogIO(Map<String, String> ops) {
        if(!BaseUtil.isBlankMap(ops)){
            String uris = HiveTypeEnum.HCATALOGMETASTOREURIS.getName();
            String db=HiveTypeEnum.HIVEDATABASE.getName();
            String table=HiveTypeEnum.HIVETABLE.getName();
            Map<String, String> configProperties = new HashMap<>();
            configProperties.put(uris,ops.get(uris));
            return HCatalogIO.read().withConfigProperties(configProperties).withDatabase(ops.get(db)).withTable(ops.get(table));
        }
        return null;
    }

    /**
     * HCatalogIO
     * @param ops
     * @param filter 过滤条件
     * @return
     */
    @Override
    public HCatalogIO.Read queryByHCatalogIO(Map<String, String> ops, String filter) {
        if(!BaseUtil.isBlankMap(ops) && BaseUtil.isNotBlank(filter)){
            return this.queryByHCatalogIO(ops).withFilter(filter);
        }
        return null;
    }

    /**
     * Row 查询
     * @param tableName
     * @param type
     * @return
     */
    @Override
    public JdbcIO.Read<Row> queryByTable(String tableName, Schema type) {
        if(BaseUtil.isBlank(tableName)){
            logger.error("tableName is null");
            return null;
        }
        if(type == null){
            logger.error("schema is null");
            return null;
        }
        String sql = "select * from "+tableName;
        return this.query(sql,type);
    }

    @Override
    public JdbcIO.Read<Map<String, ObjectCoder>> queryByTable(String tableName) {
        if(BaseUtil.isBlank(tableName)){
            logger.error("tableName is null");
            return null;
        }
        String sql = "select * from "+tableName;
        try {
            return this.selectByHive(sql);
        } catch (Exception e) {
            logger.error("query=>sql:{},msg:{}",sql,e.getMessage());
        }
        return null;
    }

    /**
     * Row查询sql
     * @param sql
     * @param type
     * @return
     */
    @Override
    public JdbcIO.Read<Row> query(String sql, Schema type) {
        try {
            return this.select(sql,type);
        } catch (Exception e) {
            logger.error("query=>sql:{},msg:{}",sql,e.getMessage());
        }
        return null;
    }


}
