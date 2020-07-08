package com.knowlegene.parent.process.io.jdbc;

import com.knowlegene.parent.process.pojo.ObjectCoder;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;

import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/8/22 14:43
 */
public interface HiveSwap {
    /**
     * 详情
     * @param tableName 表名
     * @return 结果
     */
    Schema descByTableName(String tableName, boolean isTimeStr);

    JdbcIO.Write<Map<String, ObjectCoder>> saveByIO(String sql);

    int saveCommon(String sql);


    HCatalogIO.Write saveByHCatalogIO(Map<String, String> ops, Map<String, String> partition);

    HCatalogIO.Read queryByHCatalogIO(Map<String, String> ops);

    HCatalogIO.Read queryByHCatalogIO(Map<String, String> ops, String filter);


    JdbcIO.Read<Map<String, ObjectCoder>> queryByTable(String tableName);

    JdbcIO.Read<Map<String, ObjectCoder>> query(String sql);

}
