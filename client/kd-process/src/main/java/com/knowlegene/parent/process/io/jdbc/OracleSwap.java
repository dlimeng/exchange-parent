package com.knowlegene.parent.process.io.jdbc;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/**
 * @Author: limeng
 * @Date: 2019/8/19 20:16
 */
public interface OracleSwap {


    JdbcIO.Read<Row> queryByTable(String tableName, Schema type);
    JdbcIO.Write<Row> saveByIO(String sql);
    Schema desc(String tableName);
    JdbcIO.Read<Row> query(String sql, Schema type);
}
