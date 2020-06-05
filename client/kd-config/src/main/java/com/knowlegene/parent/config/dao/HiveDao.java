package com.knowlegene.parent.config.dao;

import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/7/16 18:39
 */
public interface HiveDao {

    /**
     * 原生查询
     * 类必须序列化
     * @param type 类型
     * @param sql sql
     */
    <T>PCollection<T> list(Class<T> type, String sql);

    /**
     * 原生查询
     * @param sql sql
     */
    void list(String sql, Schema type);

    JdbcIO.Read<Row>  listByIO(String sql, Schema type);

    HCatalogIO.Read listByHCatIO(String table);

    <T>JdbcIO.Read<T>  listByIO(String sql) throws Exception;
    /**
     * 原生查询
     * @param tableName 表名
     */
    void list(String tableName);

    /**
     * 详情
     * @param sql sql
     */
    Schema desc(String sql);

    /**
     * 详情
     * @param tableName sql
     */
    Schema descByTableName(String tableName);


    HCatSchema descByTableNameAndHCat(String tableName);
    /**
     * 保存
     * @param sql
     * @return
     */
    void save(String sql, Row row);

    /**
     * 保存
     * @param sql
     * @return
     */
    JdbcIO.Write<Row> saveByIO(String sql);

    /**
     * 保存
     * @param sql
     * @return
     */
    <T>JdbcIO.Write<T> saveByIOAndSql(String sql);

    HCatalogIO.Write saveByHCatIO(String table);
    /**
     * 删除
     * @param sql
     * @return
     */
    int delete(String sql);

    /**
     * 创建表
     * @param sql sql
     * @return 结果
     */
    int createTable(String sql);

    /**
     * 保存变量
     * @param sql
     * @return
     */
    JdbcIO.Write<Row> saveByVariable(String sql, List<String> variables);

    boolean executeByHive(String commd);
}
