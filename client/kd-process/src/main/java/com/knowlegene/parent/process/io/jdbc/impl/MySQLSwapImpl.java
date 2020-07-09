package com.knowlegene.parent.process.io.jdbc.impl;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.io.jdbc.AbstractSwapBase;
import com.knowlegene.parent.process.io.jdbc.MySQLSwap;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/8/21 15:18
 */
public abstract class MySQLSwapImpl extends AbstractSwapBase implements MySQLSwap {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 查询 Row
     * @param tableName
     * @param
     * @return
     */
    @Override
    public JdbcIO.Read<Map<String, ObjectCoder>> queryByTable(String tableName) {
        String sql = "select * from "+tableName;
        return this.query(sql);
    }

    /**
     * row  sql  带批量的保存
     * @param sql
     * @return
     */
    @Override
    public JdbcIO.Write<Map<String, ObjectCoder>> saveByIO(String sql) {
        if(BaseUtil.isBlank(sql)){
            logger.error("sql is null");
            return null;
        }
        return this.batchSaveCommon(sql);
    }


    /**
     *  查询 row
     * @param sql
     * @param
     * @return
     */
    @Override
    public JdbcIO.Read<Map<String, ObjectCoder>> query(String sql) {
        try {
            return this.select(sql);
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
            return this.getSchema(tableName,false);
        }catch (Exception e){
            logger.error("desc=>tableName:{},msg:{}",tableName,e.getMessage());
        }
        return null;
    }



}
