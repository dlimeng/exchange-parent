package com.knowlegene.parent.process.transform;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.util.*;

/**
 * jdbcio
 * @Author: limeng
 * @Date: 2019/9/10 18:19
 */
public class JdbcTransform {

//    public static class HiveRowMapper implements JdbcIO.RowMapper<Row>{
//
//        @Override
//        public Row mapRow(ResultSet resultSet) throws Exception {
//            Schema build = Schema.builder().addStringField("name").addStringField("id").build();
//            return  Row.withSchema(build).addValue(resultSet.getString(1)).addValue(resultSet.getString(2)).build();
//        }
//    }

    /**
     * 查询map
     */
    public static class MapMysqlRowMapper implements JdbcIO.RowMapper<Map<String,String>>{
        @Override
        public Map<String, String> mapRow(ResultSet resultSet) throws Exception {
            return null;
        }
    }

    /**
     * 查询map
     */
    public static class MapOracleRowMapper implements JdbcIO.RowMapper<Map<String,String>>{

        @Override
        public Map<String, String> mapRow(ResultSet resultSet) throws Exception {
            return null;
        }
    }


    /**
     * hive查询map
     */
    public static class MapHiveRowMapper implements JdbcIO.RowMapper<Map<String, ObjectCoder>>{
        @Override
        public Map<String, ObjectCoder> mapRow(ResultSet resultSet) throws Exception {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            Map<String, ObjectCoder> map=null;
            if(columnCount > 0){
                map=new HashMap<>();
                for (int i = 0; i < columnCount; i++) {
                    String columnLabel = metaData.getColumnLabel(i+1);
                    if(BaseUtil.isNotBlank(columnLabel)){
                        ObjectCoder objectCoder = new ObjectCoder(resultSet.getString(columnLabel));
                        if(columnLabel.contains(".")){
                            columnLabel = columnLabel.split("\\.")[1];
                        }
                        map.put(columnLabel,objectCoder);
                    }
                }
            }
            return map;
        }
    }

    /**
     * hive
     */
    public static class PrepareStatementFromHiveRow implements JdbcIO.PreparedStatementSetter<Row>{
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        @Override
        public void setParameters(Row element, PreparedStatement preparedStatement) throws Exception {
            List<Object> values = element.getValues();
            int fieldCount = element.getFieldCount();
            Schema schema = element.getSchema();
            List<Schema.Field> fields = schema.getFields();
            logger.info("values start=>fieldCount:{}",fieldCount);
            if(!BaseUtil.isBlankSet(values)){
                for (int i = 0; i < values.size(); i++) {
                    Object o = values.get(i);

                    String instantName = Instant.class.getSimpleName();
                    String objecteName = o.getClass().getSimpleName();
                    if(instantName.equals(objecteName)){
                        Timestamp sqlDate =BaseUtil.instantToTimestamp(o);
                        preparedStatement.setObject(i+1,sqlDate);
                    }else{
                        preparedStatement.setObject(i+1,o);
                    }

                }
            }else{
                logger.info("sql values is null");
            }
            int i = preparedStatement.executeUpdate();
            logger.info("statement=>rowNum:{}",i);
        }
    }

    /**
     * 通用关系型数据库
     */
    public static class PrepareStatementFromRow implements JdbcIO.PreparedStatementSetter<Row>{
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        @Override
        public void setParameters(Row element, PreparedStatement preparedStatement) throws Exception {
            List<Object> values = element.getValues();
            int fieldCount = element.getFieldCount();
            logger.info("values start=>fieldCount:{}",fieldCount);
            if(!BaseUtil.isBlankSet(values)){
                for (int i = 0; i < values.size(); i++) {
                    preparedStatement.setObject(i+1,values.get(i));
                }
            }else{
                logger.info("sql values is null");
            }
        }
    }
}
