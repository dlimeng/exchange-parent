package com.knowlegene.parent.config.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * jdbcio工具
 * @Author: limeng
 * @Date: 2019/7/19 14:43
 */
public class JdbcUtil {
    private static Logger logger = LoggerFactory.getLogger(JdbcUtil.class);
    private static String isEmptyRex = "\\s*|\t|\n|\r";
    private static String selectRex="^(select)";
    private static String fromRex="(from)";
    private static String partitionRex="[#]";

    private static Pattern partitionCompile = Pattern.compile(partitionRex);
    private static Map<Integer,String> JDBCTypeMap =new HashMap<>();
    /**
     * 查询
     * @param sql sql
     * @param type Schema
     */
    public static JdbcIO.Read<Row> read(DataSource dataSource, String sql, Schema type) throws Exception{
        return JdbcIO.<Row>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create(dataSource)

                ).withCoder(SchemaCoder.of(type))
                .withQuery(sql).withFetchSize(50000)
                .withRowMapper(new JdbcIO.RowMapper<Row>(){
                    @Override
                    public Row mapRow(ResultSet resultSet) throws Exception {
                        Row resultSetRow = null;
                        //映射
                        List<Object> objects = JdbcUtil.convertListFilterNull(type,resultSet);
                        if(objects != null && objects.size() > 0){
                            resultSetRow = Row.withSchema(type).attachValues(objects).build();
                        }
                        return resultSetRow;
                    }
                } );

    }

    /**
     * 查询
     * @param sql sql
     */
    public static <T>JdbcIO.Read<T> readBySql(DataSource dataSource, String sql) {
        return JdbcIO.<T>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create(dataSource)

                ).withQuery(sql);
    }
    /**
     * 写
     * @param dataSource 数据源
     * @param sql sql
     * @return 结果
     */
    public static <T>JdbcIO.Write<T>  writeBySql(DataSource dataSource, String sql) {
        return JdbcIO.<T>write()
                .withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration.create(
                                dataSource))
                .withStatement(sql);
    }


    /**
     * 写
     * @param dataSource 数据源
     * @param sql sql
     * @return 结果
     */
    public static JdbcIO.Write<Row> writeHive(DataSource dataSource, String sql) throws Exception{
        return JdbcIO.<Row>write()
                .withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration.create(
                                dataSource).withConnectionProperties("hive"))
                .withStatement(sql)
                .withPreparedStatementSetter(
                        (element, statement) -> {
                            int i = statement.executeUpdate();
                        });
    }

    /**
     * 写
     * @param dataSource 数据源
     * @param sql sql
     * @return 结果
     */
    public static JdbcIO.Write<Row> writeCommon(DataSource dataSource, String sql) throws Exception{
        return JdbcIO.<Row>write()
                .withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration.create(
                                dataSource))
                .withStatement(sql)
                .withPreparedStatementSetter(
                        (element, statement) -> {

                        });
    }





    /**
     * 转换集合
     * @param resultSet 结果集
     * @return
     * @throws SQLException
     */
    public static List<Object> convertList(ResultSet resultSet) throws SQLException {
        List<Object> objects = null;
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        if(columnCount > 0){
            objects = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                objects.add(resultSet.getObject(i));
            }
        }
        return objects;
    }


    /**
     * hive
     * 获取Schema
     * @param resultSet 结果集
     */
    public static Schema getSchemaLabel(ResultSet resultSet,boolean isTimeStr) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        if(columnCount > 0){
            String columnLabel1 = metaData.getColumnLabel(1);
            String columnLabel2 = metaData.getColumnLabel(2);
            List<Schema.Field>  fields = new ArrayList<>();

            boolean isPatition=false;
            Map<String,String> patitionNames=new HashMap<>();

            while (resultSet.next()){
                String name = resultSet.getString(columnLabel1);
                String type = resultSet.getString(columnLabel2);

                Matcher matcher = partitionCompile.matcher(name);
                if(matcher.find()) isPatition=true;

                Schema.Field field = JdbcUtil.getSchemaField(name,type,isTimeStr);

                if(field == null) continue;
                else if(isPatition) patitionNames.put(name,type);

                fields.add(field);
            }
            if(!BaseUtil.isBlankMap(patitionNames)){
                List<Schema.Field>  tmpfields = new ArrayList<>();
                fields.forEach(f->{
                    if(!patitionNames.containsKey(f.getName())) tmpfields.add(f);
                });
                fields = tmpfields;
            }
            if(fields != null && fields.size() > 0) {
                return Schema.builder().addFields(fields).build();
            }
        }
        return null;
    }


    public static Schema.Field getSchemaField(String columnName,String columnType,boolean isTimeStr){
        Schema.Field result=null;
        if(BaseUtil.isNotBlank(columnType) && BaseUtil.isNotBlank(columnName)){
            String tmpColumnType = columnType.toLowerCase();
            String tmpColumnName = columnName.toLowerCase();
            //string
            //longtext
            String longtextNumber = "longtext";
            String textNumber = "text";
            String stringNumber = "string";
            String varcharNumber = JDBCType.VARCHAR.getName().toLowerCase();
            String longVarcharNumber = JDBCType.LONGNVARCHAR.getName().toLowerCase();
            String charNumber = JDBCType.CHAR.getName().toLowerCase();
            //BigDecimal
            String numericNumber = JDBCType.NUMERIC.getName().toLowerCase();
            String decimalNumber = JDBCType.DECIMAL.getName().toLowerCase();
            //boolean
            String bitNumber = JDBCType.BIT.getName().toLowerCase();
            String booleanNumber = JDBCType.BOOLEAN.getName().toLowerCase();
            //byte
            String tinyintNumber = JDBCType.TINYINT.getName().toLowerCase();
            //short
            String smallintNumber = JDBCType.SMALLINT.getName().toLowerCase();
            String intNumber="int";
            //int
            String integerNumber = JDBCType.INTEGER.getName().toLowerCase();
            //long
            String bigintNumber = JDBCType.BIGINT.getName().toLowerCase();
            //float
            String realNumber = JDBCType.REAL.getName().toLowerCase();
            //double
            String floatNumber = JDBCType.FLOAT.getName().toLowerCase();
            String doubleNumber = JDBCType.DOUBLE.getName().toLowerCase();
            //byte[]
            String binaryNumber = JDBCType.BINARY.getName().toLowerCase();
            String varbinaryNumber = JDBCType.VARBINARY.getName().toLowerCase();
            String longvarbinaryNumber = JDBCType.LONGVARBINARY.getName().toLowerCase();
            //date
            String dateNumber = JDBCType.DATE.getName().toLowerCase();
            //time
            String timeNumber = JDBCType.TIME.getName().toLowerCase();
            //timestamp
            String timestampNumber = JDBCType.TIMESTAMP.getName().toLowerCase();
            //clob
            String clobNumber = JDBCType.CLOB.getName().toLowerCase();
            //blob
            String blobNumber = JDBCType.BLOB.getName().toLowerCase();
            //array
            String arrayNumber = JDBCType.ARRAY.getName().toLowerCase();
            //mapping of underlying type
            String distinctNumber = JDBCType.DISTINCT.getName().toLowerCase();
            //Struct
            String structNumber = JDBCType.STRUCT.getName().toLowerCase();
            //ref
            String refNumber = JDBCType.REF.getName().toLowerCase();
            //URL
            String datalinkNumber = JDBCType.DATALINK.getName().toLowerCase();

            //string
            if(tmpColumnType.contains(stringNumber) || tmpColumnType.contains(varcharNumber)
                    ||  tmpColumnType.contains(longVarcharNumber)|| tmpColumnType.contains(charNumber)
            || tmpColumnType.contains(longtextNumber) || tmpColumnType.contains(textNumber) ){
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.STRING);

            }else if(tmpColumnType.contains(bitNumber) || tmpColumnType.contains(booleanNumber)){
                //boolean
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.BOOLEAN);

            }else if(tmpColumnType.contains(numericNumber) || tmpColumnType.contains(decimalNumber)){
                //BigDecimal
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.DECIMAL);

            }else if(tmpColumnType.contains(integerNumber) || tmpColumnType.contains(intNumber)){
                //int
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.INT32);

            }else if(tmpColumnType.contains(bigintNumber)){
                //long
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.INT64);

            }else if(tmpColumnType.contains(realNumber)) {
                //float
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.FLOAT);

            }else if(tmpColumnType.contains(floatNumber) || tmpColumnType.contains(doubleNumber)){
                //double
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.DOUBLE);

            }else if(tmpColumnType.contains(smallintNumber)){
                //short
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.INT16);

            }else if(tmpColumnType.contains(dateNumber)|| tmpColumnType.contains(timeNumber) || tmpColumnType.contains(timestampNumber)){
                //Schema.FieldType.DATETIME
                //date time timestamp
                if (isTimeStr) {
                    result = Schema.Field.of(tmpColumnName,Schema.FieldType.STRING);
                }else{
                    result = Schema.Field.of(tmpColumnName,Schema.FieldType.DATETIME);
                }

            }else if(tmpColumnType.contains(tinyintNumber)){
                //byte
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.BYTE);

            }
            return result;
        }
        return null;
    }

    public static Schema.Field getSchemaField2(String columnName,String columnType){
        Schema.Field result=null;
        if(BaseUtil.isNotBlank(columnType) && BaseUtil.isNotBlank(columnName)){
            String tmpColumnType = columnType.toLowerCase();
            String tmpColumnName = columnName.toLowerCase();
            //string
            String stringNumber = "string";
            String varcharNumber = JDBCType.VARCHAR.getName().toLowerCase();
            String longVarcharNumber = JDBCType.LONGNVARCHAR.getName().toLowerCase();
            String charNumber = JDBCType.CHAR.getName().toLowerCase();
            //BigDecimal
            String numericNumber = JDBCType.NUMERIC.getName().toLowerCase();
            String decimalNumber = JDBCType.DECIMAL.getName().toLowerCase();
            //boolean
            String bitNumber = JDBCType.BIT.getName().toLowerCase();
            String booleanNumber = JDBCType.BOOLEAN.getName().toLowerCase();
            //byte
            String tinyintNumber = JDBCType.TINYINT.getName().toLowerCase();
            //short
            String smallintNumber = JDBCType.SMALLINT.getName().toLowerCase();
            String intNumber="int";
            //int
            String integerNumber = JDBCType.INTEGER.getName().toLowerCase();
            //long
            String bigintNumber = JDBCType.BIGINT.getName().toLowerCase();
            //float
            String realNumber = JDBCType.REAL.getName().toLowerCase();
            //double
            String floatNumber = JDBCType.FLOAT.getName().toLowerCase();
            String doubleNumber = JDBCType.DOUBLE.getName().toLowerCase();
            //byte[]
            String binaryNumber = JDBCType.BINARY.getName().toLowerCase();
            String varbinaryNumber = JDBCType.VARBINARY.getName().toLowerCase();
            String longvarbinaryNumber = JDBCType.LONGVARBINARY.getName().toLowerCase();
            //date
            String dateNumber = JDBCType.DATE.getName().toLowerCase();
            //time
            String timeNumber = JDBCType.TIME.getName().toLowerCase();
            //timestamp
            String timestampNumber = JDBCType.TIMESTAMP.getName().toLowerCase();
            //clob
            String clobNumber = JDBCType.CLOB.getName().toLowerCase();
            //blob
            String blobNumber = JDBCType.BLOB.getName().toLowerCase();
            //array
            String arrayNumber = JDBCType.ARRAY.getName().toLowerCase();
            //mapping of underlying type
            String distinctNumber = JDBCType.DISTINCT.getName().toLowerCase();
            //Struct
            String structNumber = JDBCType.STRUCT.getName().toLowerCase();
            //ref
            String refNumber = JDBCType.REF.getName().toLowerCase();
            //URL
            String datalinkNumber = JDBCType.DATALINK.getName().toLowerCase();

            //string
            if(tmpColumnType.contains(stringNumber) || tmpColumnType.contains(varcharNumber)||  tmpColumnType.contains(longVarcharNumber)|| tmpColumnType.contains(charNumber)){
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.STRING);

            }else if(tmpColumnType.contains(bitNumber) || tmpColumnType.contains(booleanNumber)){
                //boolean
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.BOOLEAN);

            }else if(tmpColumnType.contains(numericNumber) || tmpColumnType.contains(decimalNumber)){
                //BigDecimal
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.DECIMAL);

            }else if(tmpColumnType.contains(integerNumber) || tmpColumnType.contains(intNumber)){
                //int
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.INT32);

            }else if(tmpColumnType.contains(bigintNumber)){
                //long
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.INT64);

            }else if(tmpColumnType.contains(realNumber)) {
                //float
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.FLOAT);

            }else if(tmpColumnType.contains(floatNumber) || tmpColumnType.contains(doubleNumber)){
                //double
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.DOUBLE);

            }else if(tmpColumnType.contains(smallintNumber)){
                //short
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.INT16);

            }else if(tmpColumnType.contains(dateNumber)|| tmpColumnType.contains(timeNumber) || tmpColumnType.contains(timestampNumber)){
                //Schema.FieldType.DATETIME
                //date time timestamp
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.DATETIME);

            }else if(tmpColumnType.contains(tinyintNumber)){
                //byte
                result = Schema.Field.of(tmpColumnName,Schema.FieldType.BYTE);

            }
            return result;
        }
        return null;
    }


    public static Schema.Field getSchemaField(String columnName,Integer columnType){
        Schema.Field field=null;
        if(columnType != null && BaseUtil.isNotBlank(columnName)){
            //string
            Integer varcharNumber = JDBCType.VARCHAR.getVendorTypeNumber();
            Integer longVarcharNumber = JDBCType.LONGNVARCHAR.getVendorTypeNumber();
            Integer charNumber = JDBCType.CHAR.getVendorTypeNumber();
            //BigDecimal
            Integer numericNumber = JDBCType.NUMERIC.getVendorTypeNumber();
            Integer decimalNumber = JDBCType.DECIMAL.getVendorTypeNumber();
            //boolean
            Integer bitNumber = JDBCType.BIT.getVendorTypeNumber();
            Integer booleanNumber = JDBCType.BOOLEAN.getVendorTypeNumber();
            //byte
            Integer tinyintNumber = JDBCType.TINYINT.getVendorTypeNumber();
            //short
            Integer smallintNumber = JDBCType.SMALLINT.getVendorTypeNumber();
            //int
            Integer integerNumber = JDBCType.INTEGER.getVendorTypeNumber();
            //long
            Integer bigintNumber = JDBCType.BIGINT.getVendorTypeNumber();
            //float
            Integer realNumber = JDBCType.REAL.getVendorTypeNumber();
            //double
            Integer floatNumber = JDBCType.FLOAT.getVendorTypeNumber();
            Integer doubleNumber = JDBCType.DOUBLE.getVendorTypeNumber();
            //byte[]
            Integer binaryNumber = JDBCType.BINARY.getVendorTypeNumber();
            Integer varbinaryNumber = JDBCType.VARBINARY.getVendorTypeNumber();
            Integer longvarbinaryNumber = JDBCType.LONGVARBINARY.getVendorTypeNumber();
            //date
            Integer dateNumber = JDBCType.DATE.getVendorTypeNumber();
            //time
            Integer timeNumber = JDBCType.TIME.getVendorTypeNumber();
            //timestamp
            Integer timestampNumber = JDBCType.TIMESTAMP.getVendorTypeNumber();
            //clob
            Integer clobNumber = JDBCType.CLOB.getVendorTypeNumber();
            //blob
            Integer blobNumber = JDBCType.BLOB.getVendorTypeNumber();
            //array
            Integer arrayNumber = JDBCType.ARRAY.getVendorTypeNumber();
            //mapping of underlying type
            Integer distinctNumber = JDBCType.DISTINCT.getVendorTypeNumber();
            //Struct
            Integer structNumber = JDBCType.STRUCT.getVendorTypeNumber();
            //ref
            Integer refNumber = JDBCType.REF.getVendorTypeNumber();
            //URL
            Integer datalinkNumber = JDBCType.DATALINK.getVendorTypeNumber();

            //string
            if(varcharNumber.equals(columnType) || longVarcharNumber.equals(columnType) || charNumber.equals(columnType)){
                field = Schema.Field.of(columnName,Schema.FieldType.STRING);

            }else if(bitNumber.equals(columnType) || booleanNumber.equals(columnType)){
                //boolean
                field = Schema.Field.of(columnName,Schema.FieldType.BOOLEAN);

            }else if(numericNumber.equals(columnType) || decimalNumber.equals(columnType)){
                //BigDecimal
                field = Schema.Field.of(columnName,Schema.FieldType.DECIMAL);

            }else if(integerNumber.equals(columnType)){
                //int
                field = Schema.Field.of(columnName,Schema.FieldType.INT32);

            }else if(bigintNumber.equals(columnType)){
                //long
                field = Schema.Field.of(columnName,Schema.FieldType.INT64);

            }else if(realNumber.equals(columnType)) {
                //float
                field = Schema.Field.of(columnName,Schema.FieldType.FLOAT);

            }else if(floatNumber.equals(columnType) || doubleNumber.equals(columnType)){
                //double
                field = Schema.Field.of(columnName,Schema.FieldType.DOUBLE);

            }else if(smallintNumber.equals(columnType)){
                //short
                field = Schema.Field.of(columnName,Schema.FieldType.INT16);

            }else if(dateNumber.equals(columnType) || timeNumber.equals(columnType) || timestampNumber.equals(columnType)){
                //date time timestamp
                field = Schema.Field.of(columnName,Schema.FieldType.DATETIME);

            }else if(tinyintNumber.equals(columnType)){
                //byte
                field = Schema.Field.of(columnName,Schema.FieldType.BYTE);

            }
            return field;
        }
        return null;
    }
    /**
     * schema类型转换
     * @param dbColumn
     * @param type
     * @return
     */
    public static Schema columnConversion(String[] dbColumn,Schema type){
        if(dbColumn != null && type != null ){
            List<Schema.Field>  fields = new ArrayList<>();
            Schema.Field field=null;
            for (String columnName:dbColumn){
                if(!columnName.contains("*")){
                    field = type.getField(columnName);
                    fields.add(field);
                }
            }
            if(fields != null && fields.size() > 0) {
                return Schema.builder().addFields(fields).build();
            }
        }
        return type;
    }

    /**
     * 类型名称
     * @param columnType 类型标记
     * @return
     */
    public static String getTypeName(Integer columnType){
        if(JDBCTypeMap == null || JDBCTypeMap.isEmpty()){
            setJDBCTypeMap();
        }
        return  JDBCTypeMap.get(columnType);
    }

    /**
     * 设置jdbc类型
     */
    private static void setJDBCTypeMap(){
        if(JDBCTypeMap == null || JDBCTypeMap.isEmpty()){
            for( JDBCType sqlType : JDBCType.class.getEnumConstants()) {
                JDBCTypeMap.put(sqlType.getVendorTypeNumber(),sqlType.getName());
            }
        }
    }


    /**
     * 创建表
     * @param sql sql
     * @return 结果
     * @throws SQLException
     */
    public static int create(DataSource dataSource,String sql) throws SQLException {
        int i = dataSource.getConnection().prepareStatement(sql).executeUpdate();
        return i;
    }

    /**
     * 初始化
     * @return
     */
    public static KV<Schema,Row> getInitWrite(){
        Schema type =
                Schema.builder().addStringField("isNull").build();
        Row build = Row.withSchema(type).addValue("true").build();
        return KV.of(type,build);
    }

    /**
     * 获取sql语句中的字符串
     * @param sql sql
     * @return
     */
    public static String[] getColumnBySqlRex(String sql){
        if(BaseUtil.isNotBlank(sql)){
            String sqlTmp = sql.toLowerCase();
            sqlTmp = sqlTmp.replaceAll(isEmptyRex,"");
            String[] selects = sqlTmp.split(selectRex);
            if(selects.length > 1){
                 return selects[1].split(fromRex)[0].split(",");
            }
        }
        return null;
    }


    public static Map<String,String> getHivePartition(String hivePartition){
        HashMap<String,String> partitionMap=null;
        if(BaseUtil.isNotBlank(hivePartition)){
            try {
                partitionMap = JSON.parseObject(hivePartition, new TypeReference<HashMap<String,String>>() {});
            }catch (Exception e){
                logger.error("hivePartition wrong format");
            }
        }
        return partitionMap;
    }

    public static String getInsertSQL(Schema schema,String tableName,String hivePartition){
        String result="";
        if(schema!=null && BaseUtil.isNotBlank(tableName)){
            List<String> fieldNames = schema.getFieldNames();
            StringBuffer sb =new StringBuffer();
            StringBuffer sb2=new StringBuffer();

            sb.append("insert into ");


            sb.append(tableName);

            boolean isPartition = false;
            Map<String, String> resultPartition = getHivePartition(hivePartition);
            if(!BaseUtil.isBlankMap(resultPartition)){
                boolean first=false;
                isPartition = true;
                sb.append(" partition( ");
                String partitions="";
                for(Map.Entry<String,String> keys:resultPartition.entrySet()){
                    if(first){
                        partitions+=",";
                    }
                    partitions+=keys.getKey()+"="+keys.getValue();
                    first = true;
                }
                sb.append(partitions+")");
            }else{
                isPartition =false;
            }

            if(!isPartition){
                sb.append("(");
            }

            sb2.append(" values(");
            boolean first = true;
            for (String fieldName:fieldNames){
                if(!first){
                    if(!isPartition)  sb.append(",");
                    sb2.append(",");
                }
                first=false;
                if(!isPartition)  sb.append(fieldName);
                sb2.append("?");
            }
            if(!isPartition)  sb.append(")");
            sb2.append(")");
            result = sb.toString()+sb2.toString();
        }
        return result;
    }

    /**
     * 获取插入语句
     * @param schema
     * @param tableName
     * @return
     */
    public static String getInsertSQL(Schema schema,String tableName){
        String result="";
        if(schema!=null && BaseUtil.isNotBlank(tableName)){
            List<String> fieldNames = schema.getFieldNames();
            StringBuffer sb =new StringBuffer();
            StringBuffer sb2=new StringBuffer();
            sb.append("insert into "+tableName+"(");
            sb2.append(" values(");
            boolean first = true;
            for (String fieldName:fieldNames){
                if(!first){
                    sb.append(",");
                    sb2.append(",");
                }
                first=false;
                sb.append(fieldName);
                sb2.append("?");
            }
            sb.append(")");
            sb2.append(")");
            result = sb.toString()+sb2.toString();
        }
        return result;
    }
    /**
     * 资源关闭
     *
     * @param rs
     * @param stmt
     * @param conn
     */
    public static void close(ResultSet rs, Statement stmt
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

    /**
     * sql结果集转换类型
     * @param resultSet
     * @return
     */
    public static HCatSchema getResultSetAndHCatSchema(ResultSet resultSet) throws SQLException, HCatException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        if (columnCount > 0) {
            String columnLabel1 = metaData.getColumnLabel(1);
            String columnLabel2 = metaData.getColumnLabel(2);
            HCatFieldSchema fieldSchema = null;
            List<HCatFieldSchema> columns = new ArrayList<>();
            while (resultSet.next()) {
                String name = resultSet.getString(columnLabel1);
                String type = resultSet.getString(columnLabel2);

                fieldSchema = gethCatFieldSchema(name,type);
                if(fieldSchema != null) columns.add(fieldSchema);
            }

            if(!BaseUtil.isBlankSet(columns)){
                return new HCatSchema(columns);
            }
        }
        return null;
    }

    public static  HCatFieldSchema gethCatFieldSchema(String fieldName,String columnType) throws HCatException {
        HCatFieldSchema result = null;
        if (BaseUtil.isNotBlank(fieldName) && BaseUtil.isNotBlank(columnType)) {
            //string
            String stringNumber = "string";
            String varcharNumber = JDBCType.VARCHAR.getName().toLowerCase();
            String longVarcharNumber = JDBCType.LONGNVARCHAR.getName().toLowerCase();
            String charNumber = JDBCType.CHAR.getName().toLowerCase();
            //BigDecimal
            String numericNumber = JDBCType.NUMERIC.getName().toLowerCase();
            String decimalNumber = JDBCType.DECIMAL.getName().toLowerCase();
            //boolean
            String bitNumber = JDBCType.BIT.getName().toLowerCase();
            String booleanNumber = JDBCType.BOOLEAN.getName().toLowerCase();
            //byte
            String tinyintNumber = JDBCType.TINYINT.getName().toLowerCase();
            //short
            String smallintNumber = JDBCType.SMALLINT.getName().toLowerCase();
            String intNumber="int";
            //int
            String integerNumber = JDBCType.INTEGER.getName().toLowerCase();
            //long
            String bigintNumber = JDBCType.BIGINT.getName().toLowerCase();
            //float
            String realNumber = JDBCType.REAL.getName().toLowerCase();
            //double
            String floatNumber = JDBCType.FLOAT.getName().toLowerCase();
            String doubleNumber = JDBCType.DOUBLE.getName().toLowerCase();
            //byte[]
            String binaryNumber = JDBCType.BINARY.getName().toLowerCase();
            String varbinaryNumber = JDBCType.VARBINARY.getName().toLowerCase();
            String longvarbinaryNumber = JDBCType.LONGVARBINARY.getName().toLowerCase();
            //date
            String dateNumber = JDBCType.DATE.getName().toLowerCase();
            //time
            String timeNumber = JDBCType.TIME.getName().toLowerCase();
            //timestamp
            String timestampNumber = JDBCType.TIMESTAMP.getName().toLowerCase();
            //clob
            String clobNumber = JDBCType.CLOB.getName().toLowerCase();
            //blob
            String blobNumber = JDBCType.BLOB.getName().toLowerCase();
            //array
            String arrayNumber = JDBCType.ARRAY.getName().toLowerCase();
            //mapping of underlying type
            String distinctNumber = JDBCType.DISTINCT.getName().toLowerCase();
            //Struct
            String structNumber = JDBCType.STRUCT.getName().toLowerCase();
            //ref
            String refNumber = JDBCType.REF.getName().toLowerCase();
            //URL
            String datalinkNumber = JDBCType.DATALINK.getName().toLowerCase();

            //string
            if(columnType.contains(stringNumber) || columnType.contains(varcharNumber)||  columnType.contains(longVarcharNumber)|| columnType.contains(charNumber)){
                result = new HCatFieldSchema(fieldName, TypeInfoFactory.stringTypeInfo, "");

            }else if(columnType.contains(bitNumber) || columnType.contains(booleanNumber)){
                //boolean
                result = new HCatFieldSchema(fieldName, TypeInfoFactory.booleanTypeInfo, "");

            }else if(columnType.contains(numericNumber) || columnType.contains(decimalNumber)){
                //BigDecimal
                result = new HCatFieldSchema(fieldName, TypeInfoFactory.decimalTypeInfo, "");

            }else if(columnType.contains(integerNumber) || columnType.contains(intNumber)){
                //int
                result = new HCatFieldSchema(fieldName, TypeInfoFactory.intTypeInfo, "");

            }else if(columnType.contains(bigintNumber)){
                //long
                result = new HCatFieldSchema(fieldName, TypeInfoFactory.longTypeInfo, "");

            }else if(columnType.contains(realNumber)) {
                //float
                result =new HCatFieldSchema(fieldName, TypeInfoFactory.floatTypeInfo, "");

            }else if(columnType.contains(floatNumber) || columnType.contains(doubleNumber)){
                //double
                result = new HCatFieldSchema(fieldName, TypeInfoFactory.doubleTypeInfo, "");

            }else if(columnType.contains(smallintNumber)){
                //short
                result = new HCatFieldSchema(fieldName, TypeInfoFactory.shortTypeInfo, "");

            }else if(columnType.contains(dateNumber)|| columnType.contains(timeNumber) || columnType.contains(timestampNumber)){
                //date time timestamp
                result = new HCatFieldSchema(fieldName, TypeInfoFactory.timestampTypeInfo, "");

            }else if(columnType.contains(tinyintNumber)){
                //byte
                result = new HCatFieldSchema(fieldName, TypeInfoFactory.byteTypeInfo, "");

            }
            return result;
        }


        return result;
    }

    public static List<Object> convertListFilterNull(Schema type,ResultSet resultSet) throws SQLException {
        List<Object> objects = new ArrayList<>();
        for (Schema.Field field:type.getFields()){
            String name_type=field.getType().getTypeName().name();
            Object object = resultSet.getObject(field.getName());
            object=converJodaTypeFilterNull(object,name_type);
            objects.add(object);
        }
        return objects;
    }

    public static Object converJodaTypeFilterNull(Object object,String type){
        String simpleName =type.toLowerCase();
        Object result=object;
        //date
        String dateNumber = JDBCType.DATE.getName().toLowerCase();
        //time
        String timeNumber = JDBCType.TIME.getName().toLowerCase();
        //timestamp
        String timestampNumber = JDBCType.TIMESTAMP.getName().toLowerCase();

        String datatime = "datetime";

        if(simpleName.contains(dateNumber) || simpleName.contains(timeNumber) || simpleName.contains(timestampNumber) || simpleName.contains(datatime)){

            if (object==null){
                result="";
            }else {
                result = result.toString();
            }
        }else if(simpleName.contains("bigdecimal")){
            if (result==null){
                result=0;
            }else {
                BigDecimal bg = new BigDecimal(result.toString());
                result = bg.doubleValue();
            }
        }else if(simpleName.contains("double")){
            if (object==null){
                result=0.0;
            }else{
                BigDecimal bg = new BigDecimal(result.toString());
                result = bg.doubleValue();
            }
        }else  if(simpleName.contains("string")){
            if (object==null) {
                result = "";
            }else{
                result=result.toString();
            }
        }else {
            if (object==null) {
                result = 0;
            }
        }
        return result;
    }

}
