package com.knowlegene.parent.process.io.jdbc;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/**
 * @Author: limeng
 * @Date: 2019/8/22 11:53
 */
public interface MySQLSwap {

    JdbcIO.Read<Row> queryByTable(String tableName, Schema type);
    JdbcIO.Write<Row> saveByIO(String sql);

    Schema desc(String tableName);
    JdbcIO.Read<Row> query(String sql, Schema type);
}
