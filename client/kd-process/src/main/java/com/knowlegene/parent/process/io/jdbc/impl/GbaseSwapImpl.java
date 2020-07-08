package com.knowlegene.parent.process.io.jdbc.impl;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.io.jdbc.AbstractSwapBase;
import com.knowlegene.parent.process.io.jdbc.GbaseSwap;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;


import javax.sql.DataSource;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/10/12 14:16
 */
public abstract class GbaseSwapImpl extends AbstractSwapBase implements GbaseSwap {

    @Override
    public JdbcIO.Read<Map<String, ObjectCoder>> queryByTable(String tableName) {
        String sql = "select * from "+tableName;
        return this.query(sql);
    }

    @Override
    public Schema desc(String tableName) {
        try {
            return this.getSchema(tableName);
        }catch (Exception e){
            getLogger().error("desc=>tableName:{},msg:{}",tableName,e.getMessage());
        }
        return null;
    }

    @Override
    public JdbcIO.Read<Map<String, ObjectCoder>> query(String sql) {
        try {
            return this.select(sql);
        } catch (Exception e) {
            getLogger().error("query=>sql:{},msg:{}",sql,e.getMessage());
        }
        return null;
    }


    @Override
    public JdbcIO.Write<Map<String, ObjectCoder>> saveByIO(String sql) {
        if(BaseUtil.isBlank(sql)){
            getLogger().error("sql is null");
            return null;
        }
        return this.batchSaveCommon(sql);
    }

}
