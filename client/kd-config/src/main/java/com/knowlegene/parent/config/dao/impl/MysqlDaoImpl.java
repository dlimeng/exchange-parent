package com.knowlegene.parent.config.dao.impl;

import com.knowlegene.parent.config.dao.AbstractSqlBaseDaoImpl;
import com.knowlegene.parent.config.dao.MysqlDao;

import com.knowlegene.parent.config.deploy.MysqlConfig;
import com.knowlegene.parent.config.util.PipelineSingletonUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

/**
 * @Author: limeng
 * @Date: 2019/7/15 18:56
 */

public class MysqlDaoImpl extends AbstractSqlBaseDaoImpl implements MysqlDao {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Resource(name="mysqlDataSource")
    private DataSource mysqlDataSource;
    @Override
    public DataSource getDataSource() {
        return new MysqlConfig().getMysqlDataSource1();
    }

    @Override
    public Pipeline getPipeline() {
        return PipelineSingletonUtil.instance;
    }

    @Override
    public void list(String sql, Schema type) {
        getPipeline().apply(this.query(sql, type));
    }

    @Override
    public JdbcIO.Read<Row> listByIO(String sql, Schema type) {
        return this.query(sql, type);
    }

    @Override
    public <T> JdbcIO.Read<T> listByIO(String sql) {
        return this.read(sql);
    }

    @Override
    public void list(String tableName) {
        Schema schema = this.descByTableName(tableName);
        if(schema != null){
            String sql = "select * from "+tableName;
            this.list(sql,schema);
        }
    }

    @Override
    public Schema desc(String sql) {
        try {
            return this.description(sql);
        } catch (SQLException e) {
            logger.error("MysqlDao desc error=>msg:{}",e.getMessage());
        }
        return null;
    }

    @Override
    public Schema descByTableName(String tableName) {
        String sql="desc "+tableName;
        return this.desc(sql);
    }

    @Override
    public void save(String sql, Row row) {
        getPipeline().apply(Create.of(row))
                .apply(Objects.requireNonNull(this.writeBySql(sql)));
    }

    @Override
    public JdbcIO.Write<Row> saveByIO(String sql) {
        return this.writeBySql(sql);
    }

    @Override
    public <T> JdbcIO.Write<T> saveByIOAndSql(String sql) {
        return this.writeByModel(sql);
    }

    @Override
    public int delete(String sql) {
        try {
            return this.del(sql);
        } catch (SQLException e) {
            logger.error("MysqlDao delete error=>msg:{}",e.getMessage());
        }
        return -1;
    }

    @Override
    public int createTable(String sql) {
        try {
            return this.create(sql);
        } catch (SQLException e) {
            logger.error("MysqlDao createTable error=>msg:{}",e.getMessage());
        }
        return -1;
    }

    /**
     * 查询
     * @param sql sql
     * @param type 字段类型
     * @return
     */

    private JdbcIO.Read<Row> query(String sql, Schema type){
        try {
            return this.read(sql,type);
        } catch (Exception e) {
            logger.error("MysqlDao JdbcIO.Read<Row> error=>msg:{},sql:{}",e.getMessage(),sql);
        }
        return null;
    }

    /**
     *
     * @param sql
     * @return
     */
    private JdbcIO.Write<Row> writeBySql(String sql){
        try {
            return this.writeCommon(sql);
        } catch (Exception e) {
            logger.error("MysqlDao JdbcIO.Write<Row> error=>msg:{},sql:{}",e.getMessage(),sql);
        }
        return null;
    }
}
