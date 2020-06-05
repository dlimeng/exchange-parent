package com.knowlegene.parent.process.io.jdbc.impl;

import com.knowlegene.parent.process.io.jdbc.AbstractSwapBase;
import com.knowlegene.parent.process.io.jdbc.GbaseSwap;
import com.knowlegene.parent.process.util.GbaseDataSourceUtil;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;


import javax.sql.DataSource;

/**
 * @Author: limeng
 * @Date: 2019/10/12 14:16
 */
public class GbaseSwapImpl extends AbstractSwapBase implements GbaseSwap {

    @Override
    public JdbcIO.Read<Row> queryByTable(String tableName, Schema type) {
        String sql = "select * from "+tableName;
        return this.query(sql,type);
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
    public JdbcIO.Read<Row> query(String sql, Schema type) {
        try {
            return this.select(sql,type);
        } catch (Exception e) {
            getLogger().error("query=>sql:{},msg:{}",sql,e.getMessage());
        }
        return null;
    }

    @Override
    public DataSource getDataSource() {
        return GbaseDataSourceUtil.getDataSource();
    }

}
