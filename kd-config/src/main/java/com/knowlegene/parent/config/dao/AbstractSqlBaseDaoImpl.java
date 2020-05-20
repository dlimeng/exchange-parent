package com.knowlegene.parent.config.dao;

import com.knowlegene.parent.config.util.JdbcUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;

/**
 * 公共数据源
 * @Author: limeng
 * @Date: 2019/7/22 14:37
 */
public abstract class AbstractSqlBaseDaoImpl {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * 获取数据源
     * @return
     */
    protected abstract DataSource getDataSource();

    public abstract Pipeline getPipeline();

    private  Connection connection;

    public Connection getConnection(){
        DataSource dataSource = this.getDataSource();
        if(dataSource != null){
            try {
                connection = dataSource.getConnection();
            } catch (SQLException e) {
                logger.error("getConnection error=>msg:{}",e.getMessage());
            }
        }
        return connection;
    }

    /**
     * 获取字段类型
     * @param sql sql
     * @return
     * @throws SQLException
     */
    public Schema description(String sql) throws SQLException {
        DataSource dataSource = this.getDataSource();
        if(dataSource!= null){
            this.getConnection();
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            Schema schemaLabel = JdbcUtil.getSchemaLabel(resultSet,false);
            close(resultSet,ps,connection);
            return schemaLabel;
        }
        return null;
    }



    /**
     * 删除
     * @param sql sql
     * @return 影响行数
     * @throws SQLException
     */
    public int del(String sql) throws SQLException {
        this.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        int i = preparedStatement.executeUpdate();
        this.close(null,preparedStatement,connection);
        return i;
    }


    /**
     * 创建表
     * @param sql sql
     * @return 结果
     * @throws SQLException
     */
    public int create(String sql) throws SQLException {
        this.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        int i = preparedStatement.executeUpdate();
        this.close(null,preparedStatement,connection);
        return i;
    }

    /**
     * 执行命令
     * @param commd
     * @return
     */
    public boolean execute(String commd){
        boolean result = false;
        this.getConnection();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            result = statement.execute(commd);
        } catch (SQLException e) {
            logger.error("execute error=>msg:{}",e.getMessage());
        }finally {
            this.close(null,statement,connection);
        }
        return result;
    }

    /**
     * 读
     * @param sql sql
     * @param type 字段类型
     * @return
     * @throws Exception
     */
    public JdbcIO.Read<Row> read(String sql, Schema type) throws Exception {
        DataSource dataSource = this.getDataSource();
        if(dataSource!= null){
            return JdbcUtil.read(dataSource, sql, type);
        }
        return null;
    }

    /**
     * 缺code 赋值
     * @param sql sql
     * @param <T> 类型
     * @return r
     * @throws Exception
     */
    public <T>JdbcIO.Read<T> read(String sql) {
        DataSource dataSource = getDataSource();
        if(dataSource!= null){
            return JdbcUtil.readBySql(dataSource, sql);
        }
        return null;
    }
    /**
     * 写
     * @param sql sql
     * @return 结果
     */
    public JdbcIO.Write<Row> writeHive(String sql) throws Exception {
        DataSource dataSource = getDataSource();
        if(dataSource!= null){
            return JdbcUtil.<Row>writeHive(dataSource, sql);
        }
        return null;
    }

    /**
     * 写
     * @param sql sql
     * @return 结果
     */
    public JdbcIO.Write<Row> writeCommon(String sql) throws Exception {
        DataSource dataSource = getDataSource();
        if(dataSource!= null){
            return JdbcUtil.<Row>writeCommon(dataSource, sql);
        }
        return null;
    }

    public <T>JdbcIO.Write<T> writeByModel(String sql)  {
        DataSource dataSource = getDataSource();
        if(dataSource!= null){
            return JdbcUtil.writeBySql(dataSource, sql);
        }
        return null;
    }


    /**
     * 资源关闭
     *
     * @param rs
     * @param stmt
     * @param conn
     */
    public void close(ResultSet rs, Statement stmt
            , Connection conn) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
