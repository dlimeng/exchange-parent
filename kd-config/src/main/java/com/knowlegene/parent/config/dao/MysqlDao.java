package com.knowlegene.parent.config.dao;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * @Author: limeng
 * @Date: 2019/7/15 18:55
 */
public interface MysqlDao {


    /**
     * 原生查询
     * @param sql sql
     */
    void list(String sql, Schema type);

    /**
     * 查询
     * @param sql
     * @param type
     * @return
     */
    JdbcIO.Read<Row>  listByIO(String sql, Schema type);

    <T>JdbcIO.Read<T>  listByIO(String sql);
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
    <T>JdbcIO.Write<T> saveByIOAndSql(String sql);
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
}
