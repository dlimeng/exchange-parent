package com.knowlegene.parent.process.io.jdbc;

import com.knowlegene.parent.process.pojo.ObjectCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/8/19 20:16
 */
public interface OracleSwap {


    JdbcIO.Read<Map<String, ObjectCoder>> queryByTable(String tableName);
    JdbcIO.Write<Map<String, ObjectCoder>> saveByIO(String sql);
    Schema desc(String tableName);
    JdbcIO.Read<Map<String, ObjectCoder>> query(String sql);
}
