package com.knowlegene.parent.process.io.jdbc;

import com.knowlegene.parent.process.pojo.ObjectCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/10/12 14:06
 */
public interface GbaseSwap {
    JdbcIO.Read<Map<String, ObjectCoder>> queryByTable(String tableName);

    Schema desc(String tableName);

    JdbcIO.Write<Map<String, ObjectCoder>> saveByIO(String sql);

    JdbcIO.Read<Map<String, ObjectCoder>> query(String sql);
}
