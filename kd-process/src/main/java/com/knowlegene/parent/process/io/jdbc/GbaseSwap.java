package com.knowlegene.parent.process.io.jdbc;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/**
 * @Author: limeng
 * @Date: 2019/10/12 14:06
 */
public interface GbaseSwap {
    JdbcIO.Read<Row> queryByTable(String tableName, Schema type);

    Schema desc(String tableName);

    JdbcIO.Read<Row> query(String sql, Schema type);
}
