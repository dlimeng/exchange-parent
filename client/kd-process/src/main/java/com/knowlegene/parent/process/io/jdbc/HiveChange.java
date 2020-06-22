package com.knowlegene.parent.process.io.jdbc;

import com.knowlegene.parent.config.dao.BaseDao;
import com.knowlegene.parent.config.pojo.sql.SQLResult;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.util.List;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/7/22 18:57
 */
public interface HiveChange extends BaseDao {
    /**
     * 创建表
     * @return 影响行数
     */
     int create(String sql);

    /**
     * 查询
     * @param sql sql
     * @param type 字段类型
     */
     void list(String sql, Schema type);

    /**
     * 查询
     * @param sql sql
     * @param type 字段类型
     * @return
     */
     JdbcIO.Read<Row> listByIO(String sql, Schema type);

    HCatalogIO.Read listByHCatIO(String table, String dbName);
    /**
     * 详情
     * @param sql sql
     * @return 返回结果
     */
      Schema desc(String sql);

    /**
     * 详情
     * @param tableName 表名
     * @return 结果
     */
     Schema descByTableName(String tableName);

     HCatSchema descByHCatSchema(String tableName);
    /**
     * 保存
     * @param sql
     */
     void save(String sql, Row row);


    public JdbcIO.Write<Row> saveByIO(String sql);
    /**
     * 删除
     * @param sql
     * @return
     */
    int delete(String sql);

    /**
     * 分配整个链路任务
     * @param sql
     */
    PCollection<String> distributionLink(PCollection<String> sql);

    /**
     * 定义语句
     * with
     * set
     * create
     * truncate
     * @return 影响行数
     */
    PCollection<String> ddl(PCollection<String> sql);

    /**
     * 无序
     * @param sql
     * @return
     */
    PCollection<String> save(PCollection<String> sql);


    PCollection<String> save(PCollection<Row> sql, Schema type);
    /**
     * 保存有序
     *
     */
    List<String> saveByOrder(List<String> sql, boolean isOrder);

    /**
     * @param sql
     * @param isOrder
     * @return
     */
    List<String> ddlByOrder(List<String> sql, boolean isOrder);

    /**
     * 保存有序
     *
     */
    void saveByOrder(SQLResult resultDO);

    /**
     * @return
     */
    void ddlByOrder(SQLResult resultDO);

    HCatalogIO.Write saveByHCatIO(String table, String dbName);

    JdbcIO.Read<Map<String, ObjectCoder>> queryByTable(String sql);

    JdbcIO.Write<Row> batchHiveSave(String sql);

}
