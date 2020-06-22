package com.knowlegene.parent.process.io.jdbc.impl;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.io.jdbc.AbstractSwapBase;
import com.knowlegene.parent.process.io.jdbc.MySQLSwap;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * @Author: limeng
 * @Date: 2019/8/21 15:18
 */
public abstract class MySQLSwapImpl extends AbstractSwapBase implements MySQLSwap {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 查询 Row
     * @param tableName
     * @param type
     * @return
     */
    @Override
    public JdbcIO.Read<Row> queryByTable(String tableName, Schema type) {
        String sql = "select * from "+tableName;
        return this.query(sql,type);
    }

    /**
     * row  sql  带批量的保存
     * @param sql
     * @return
     */
    @Override
    public JdbcIO.Write<Row> saveByIO(String sql) {
        if(BaseUtil.isBlank(sql)){
            logger.error("sql is null");
            return null;
        }
        return this.batchSaveCommon(sql);
    }

    /**
     *  查询 row
     * @param sql
     * @param type
     * @return
     */
    @Override
    public JdbcIO.Read<Row> query(String sql, Schema type) {
        try {
            return this.select(sql,type);
        } catch (Exception e) {
            logger.error("query=>sql:{},msg:{}",sql,e.getMessage());
        }
        return null;
    }

    /**
     * 详情
     * @param tableName 表名
     * @return
     */
    @Override
    public Schema desc(String tableName) {
        try {
            return this.getSchema(tableName);
        }catch (Exception e){
            logger.error("desc=>tableName:{},msg:{}",tableName,e.getMessage());
        }
        return null;
    }



}
