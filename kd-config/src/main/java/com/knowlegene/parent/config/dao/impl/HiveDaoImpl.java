package com.knowlegene.parent.config.dao.impl;

import com.knowlegene.parent.config.common.constantenum.HiveTypeEnum;
import com.knowlegene.parent.config.dao.AbstractSqlBaseDaoImpl;
import com.knowlegene.parent.config.dao.HiveDao;
import com.knowlegene.parent.config.deploy.HiveConfig;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import com.knowlegene.parent.config.util.PipelineSingletonUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * hive
 * @Author: limeng
 * @Date: 2019/7/16 18:39
 */
public class HiveDaoImpl extends AbstractSqlBaseDaoImpl implements HiveDao  {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String MARK_SPLIT = "\\|%a%b%c%\\|";
    private static final String ID_T = "id";
    private static final String INDEX_T = "index";
    private static final String COMMA = ", ";
    private static final String OPERATING_COUNT = " COUNT(*) ";
    private static final String UNDERLINE_T = "_";


    @Resource(name = "hiveDataSource")
    private DataSource hiveDataSource;

    @Resource(name = "hiveMetastore")
    private Map<String, String> hiveMetastore;

    @Override
    public <T> PCollection<T> list(Class<T> type, String sql) {

        return null;
    }

    /**
     * 查询
     * @param sql sql
     * @param type 类型
     * @return 结果
     */
    @Override
    public void list(String sql,Schema type) {
        getPipeline().apply(this.query(sql, type));
    }

    @Override
    public JdbcIO.Read<Row> listByIO(String sql, Schema type) {
        return this.query(sql, type);
    }

    @Override
    public HCatalogIO.Read listByHCatIO(String table) {
        if(BaseUtil.isNotBlank(table)){
            String uris = HiveTypeEnum.HCATALOGMETASTOREURIS.getName();
            String db=HiveTypeEnum.HIVEDATABASE.getName();
            Map<String, String> configProperties = new HashMap<>();
            configProperties.put(uris,hiveMetastore.get(uris));
            return HCatalogIO.read().withConfigProperties(configProperties).withDatabase(hiveMetastore.get(db)).withTable(table);
        }
        return null;
    }

    @Override
    public <T> JdbcIO.Read<T> listByIO(String sql) {
        return this.read(sql);
    }

    /**
     * 查询
     * @param tableName 表名
     * @return
     */
    @Override
    public void list(String tableName) {
        Schema schema = this.descByTableName(tableName);
        if(schema != null){
            String sql = "select * from "+tableName;
            this.list(sql,schema);
        }
    }

    /**
     * 获取字段
     * @param sql sql
     * @return 返回
     */
    @Override
    public Schema desc(String sql) {
        try {
            return this.description(sql);
        } catch (SQLException e) {
            logger.error("desc error=>msg:{}",e.getMessage());
        }
        return null;
    }

    /**
     * 详情
     * @param tableName sql
     * @return 返回
     */
    @Override
    public Schema descByTableName(String tableName) {
        String sql="desc "+tableName;
        return this.desc(sql);
    }

    @Override
    public HCatSchema descByTableNameAndHCat(String tableName) {
        DataSource dataSource = this.getDataSource();
        if(dataSource!= null && BaseUtil.isNotBlank(tableName)){
            String sql="desc "+tableName;
            Connection connection = getConnection();
            try {
                PreparedStatement ps = connection.prepareStatement(sql);
                ResultSet resultSet = ps.executeQuery();
                HCatSchema result = JdbcUtil.getResultSetAndHCatSchema(resultSet);
                close(resultSet,ps,connection);
                return result;
            } catch (Exception e) {
                logger.error("getHCatSchema is error=>sql:{}",sql);
            }
        }
        return null;
    }


    /**
     * 保存
     * @param sql 语句
     * @return 结果
     */
    @Override
    public void save(String sql,Row row) {
       getPipeline().apply(Create.of(row))
                    .apply(this.writeBySql(sql));
    }

    /**
     * 保存
     * @param sql sql
     * @return
     */
    @Override
    public JdbcIO.Write<Row> saveByIO(String sql) {
        return this.writeBySql(sql);
    }

    @Override
    public <T> JdbcIO.Write<T> saveByIOAndSql(String sql) {
        return this.writeByModel(sql);
    }

    @Override
    public HCatalogIO.Write saveByHCatIO(String table) {
        if(BaseUtil.isNotBlank(table)){
            String uris = HiveTypeEnum.HCATALOGMETASTOREURIS.getName();
            String db=HiveTypeEnum.HIVEDATABASE.getName();
            Map<String, String> configProperties = new HashMap<>();
            configProperties.put(uris,hiveMetastore.get(uris));

            return HCatalogIO.write().withConfigProperties(configProperties).withDatabase(hiveMetastore.get(db)).withTable(table).withBatchSize(1024L);
        }
        return null;
    }


    @Override
    public int delete(String sql) {
        try {
            return this.del(sql);
        } catch (SQLException e) {
            logger.error("delete error=>msg:{}",e.getMessage());
        }
        return -1;
    }

    @Override
    public int createTable(String sql) {
        try {
            return this.create(sql);
        } catch (SQLException e) {
            logger.error("createTable error=>msg:{}",e.getMessage());
        }
        return -1;
    }

    @Override
    public JdbcIO.Write<Row> saveByVariable(String sql,List<String> variables) {
        JdbcIO.Write<Row> rowWrite = JdbcIO.<Row>write()
                .withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration.create(
                                getDataSource()))
                .withStatement(sql)
                .withPreparedStatementSetter(
                        (element, statement) -> {
                            if (!BaseUtil.isBlankSet(variables)) {
                                Iterator<String> iterator = variables.iterator();
                                while (iterator.hasNext()) {
                                    String next = iterator.next();
                                    int i = statement.executeUpdate(next);

                                }
                            }
                            int i = statement.executeUpdate();
                        });

        return rowWrite;

    }

    @Override
    public boolean executeByHive(String commd) {
        if(BaseUtil.isNotBlank(commd)){
            return super.execute(commd);
        }
        return false;
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
            logger.error("JdbcIO.Read<Row> error=>msg:{},sql:{}",e.getMessage(),sql);
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
            return writeHive(sql);
        } catch (Exception e) {
            logger.error("JdbcIO.Write<Row> error=>msg:{},sql:{}",e.getMessage(),sql);
        }
        return null;
    }


    /***
     * 将map中的key的名称修改为根据某个标记编辑成驼峰的形式，并通过jsonUtil返回成class对象
     * @param map map
     * @param type type
     * @param <T> 泛型
     * @return return
     */
    private <T> T getResult(Map<String, Object> map, Class<T> type){
        //去除三方插件的字段
        map.remove("logger");
        return BaseUtil.JsonUtils.jsonToHumpToObject(map, type.getSimpleName().equals("Map") ? HiveDaoImpl.MARK_SPLIT : HiveDaoImpl.UNDERLINE_T, type);
    }



    /**
     * 获取当前数据源
     * @return 结果
     */
    @Override
    public DataSource getDataSource(){
        return new HiveConfig().getHiveDataSource1();
    }

    @Override
    public Pipeline getPipeline() {
        return PipelineSingletonUtil.instance;
    }

}
