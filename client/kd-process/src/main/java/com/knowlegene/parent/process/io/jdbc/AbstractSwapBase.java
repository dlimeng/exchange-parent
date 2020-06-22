package com.knowlegene.parent.process.io.jdbc;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.transform.JdbcTransform;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/8/22 14:49
 */
public abstract class AbstractSwapBase implements Serializable {
    private Logger logger = null;

    public Logger getLogger() {
        return logger;
    }

    public AbstractSwapBase() {
        logger = LoggerFactory.getLogger(this.getClass());
    }

    public abstract DataSource getDataSource();

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
     * 查询
     * @param sql sql
     * @param type 类型
     * @return
     * @throws Exception
     */
    public JdbcIO.Read<Row> select(String sql, Schema type) throws Exception {
        DataSource dataSource = this.getDataSource();
        if(dataSource!= null){
            return JdbcUtil.read(dataSource,sql,type);
        }
        return null;
    }



    /**
     * hive查询
     * @param sql sql
     * @return
     * @throws Exception
     */
    public JdbcIO.Read<Map<String, ObjectCoder>> selectByHive(String sql) throws Exception {
        DataSource dataSource = this.getDataSource();
        if(dataSource!= null){
            return JdbcIO.<Map<String,ObjectCoder>>read().
                    withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                            .create(dataSource)

                    ).withCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ObjectCoder.class)))
                    .withQuery(sql).withFetchSize(10000)
                    .withRowMapper(new JdbcTransform.MapHiveRowMapper());
        }
        return null;
    }

    /**
     * 普通获取类型  关系型数据
     * @param tableName
     * @return
     * @throws SQLException
     */
    public Schema getSchema(String tableName,boolean isTimeStr) throws SQLException {
        DataSource dataSource = getDataSource();
        if(BaseUtil.isNotBlank(tableName) && dataSource!=null){
            String sql="desc "+tableName;
            getLogger().info("Schema start =>tableName:{}",tableName);
            getConnection();
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();

            if(metaData.getColumnCount() > 0){
                String columnLabel1 = metaData.getColumnLabel(1);
                String columnLabel2 = metaData.getColumnLabel(2);
                List<Schema.Field>  fields = new ArrayList<>();

                while (resultSet.next()){
                    String name = resultSet.getString(columnLabel1);
                    String type = resultSet.getString(columnLabel2);
                    Schema.Field  field = JdbcUtil.getSchemaField(name,type,isTimeStr);
                    if(field == null) continue;
                    fields.add(field);
                }
                close(resultSet,ps,connection);
                if(fields != null && fields.size() > 0) {
                    return Schema.builder().addFields(fields).build();
                }
            }
        }
        return null;
    }

    /**
     * 普通获取类型  关系型数据
     * @param tableName
     * @return
     * @throws SQLException
     */
    public Schema getSchema(String tableName) throws SQLException {
        return getSchema(tableName,true);
    }

    /**
     * 普通获取类型  oracle关系型数据
     * @param tableName
     * @return
     * @throws SQLException
     */
    public Schema getOracleSchema(String tableName) throws SQLException {
        return getOracleSchema(tableName,true);
    }
    /**
     * 普通获取类型  oracle关系型数据
     * @param tableName
     * @return
     * @throws SQLException
     */
    public Schema getOracleSchema(String tableName,boolean isTimeStr) throws SQLException {
        DataSource dataSource = getDataSource();
        if(BaseUtil.isNotBlank(tableName) && dataSource!=null){
            String sql="select u.column_name,u.data_type from user_tab_columns u where u.table_name='"+tableName.toUpperCase()+"' order by u.column_name ";
            getLogger().info("Schema start =>tableName:{}",tableName);
            getConnection();
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            List<Schema.Field>  fields = new ArrayList<>();
            while (resultSet.next()){
                String columnName = resultSet.getString(1);
                String dataType = resultSet.getString(2);
                Schema.Field  field = JdbcUtil.getSchemaField(columnName,dataType,isTimeStr);
                if(field == null) continue;
                fields.add(field);
            }
            close(resultSet,ps,connection);
            if(fields != null && fields.size() > 0) {
                return Schema.builder().addFields(fields).build();
            }
        }
        return null;
    }

    /**
     * 通过表名  获取类型 hive
     * @param tableName
     * @return
     * @throws SQLException
     */
    public Schema getHiveSchema(String tableName,boolean isTimeStr) throws SQLException {
        DataSource dataSource = getDataSource();
        if(BaseUtil.isNotBlank(tableName) && dataSource!=null){
            getLogger().info("HiveSchema start =>table:{}",tableName);
            String sql="desc "+tableName;
            getConnection();
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            Schema schema = JdbcUtil.getSchemaLabel(resultSet,isTimeStr);

            close(resultSet,ps,connection);
            return schema;
        }else{
            getLogger().error("dataSource is null");
        }
        return null;
    }

    /**
     * 批量保存 hive
     * @param sql
     * @return
     */
    public JdbcIO.Write<Row> batchHiveSave(String sql){
        DataSource dataSource = this.getDataSource();
        if(dataSource!= null){
            return JdbcIO.<Row>write()
                    .withDataSourceConfiguration(
                            JdbcIO.DataSourceConfiguration.create(
                                    dataSource).withConnectionProperties("hive"))
                    .withStatement(sql)
                    .withPreparedStatementSetter(new JdbcTransform.PrepareStatementFromHiveRow());
        }
        return null;
    }


    /**
     * 批量保存 关系型数据
     * @param sql
     * @return
     */
    public JdbcIO.Write<Row> batchSaveCommon(String sql){
        DataSource dataSource = this.getDataSource();
        if(dataSource!= null){
            return JdbcIO.<Row>write()
                    .withDataSourceConfiguration(
                            JdbcIO.DataSourceConfiguration.create(
                                    dataSource))
                    .withStatement(sql)
                    .withPreparedStatementSetter(new JdbcTransform.PrepareStatementFromRow());
        }
        return null;
    }


    /**
     * 创建表
     * @param sql sql
     * @return 结果
     * @throws SQLException
     */
    public int write(String sql) throws SQLException {
        this.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        int i = preparedStatement.executeUpdate();
        this.close(null,preparedStatement,connection);
        return i;
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
