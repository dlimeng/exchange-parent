package com.knowlegene.parent.process.io.jdbc;

import com.alibaba.fastjson.JSON;
import com.knowlegene.parent.config.pojo.sql.SQLOperators;
import com.knowlegene.parent.config.pojo.sql.SQLResult;
import com.knowlegene.parent.config.util.PipelineSingletonUtil;
import com.knowlegene.parent.process.pojo.SQLOptions;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.hcatalog.common.HCatException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.beans.PropertyVetoException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * hive 原子操作
 * @Author: limeng
 * @Date: 2019/8/8 21:47
 */
public  class HiveChangeTest implements Serializable {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static HiveChange hiveChange;
    /**
     * 设置参数
     */
    @Before
    public  void getPipeline(){
        //初始化applicationContext
//        SpringContextUtil springContextUtils = new SpringContextUtil();
//        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext(this.getClass());
//        springContextUtils.setApplicationContext(applicationContext);
//        hiveChange = applicationContext.getBean(HiveChange.class);
//        PipelineOptions options = PipelineOptionsFactory.create();
//        options.setRunner(DirectRunner.class);
//        Pipeline pipeline = Pipeline.create(options);
//        PipelineSingletonUtil.getInstance(pipeline);

    }
    @After
    public  void run(){
        //Pipeline pipeline = PipelineSingletonUtil.instance;
        //pipeline.run().waitUntilFinish();
    }
    /**
     * 无序
     * 插入
     */
    @Test
    public  void save(){
        String sql="insert into a_mart.test values(\"444\",\"text\")";
        List<String> list= Collections.singletonList(sql);
        Pipeline pipeline = PipelineSingletonUtil.instance;
        PCollection<String> stringPCollection = pipeline.apply(Create.of(list)).setCoder(StringUtf8Coder.of());
        hiveChange.save(stringPCollection).setCoder(StringUtf8Coder.of());
    }
    /**
     * 保存
     *
     */
    @Test
    public void saveByModel(){
        String json2="{\"optionType\":\"insert\",\"insert\":\"insert overwrite table  a_mart.test2019\",\"select\":\"select id,name\",\"from\":\"from a_mart.test2019\"}";
        SQLOptions sqlOptionsDTO = JSON.parseObject(json2, SQLOptions.class);
        SQLResult sqlResultDO = new SQLResult();
        sqlResultDO.insert(new SQLOperators().addInsert(sqlOptionsDTO.getInsert())
                .addSelect(sqlOptionsDTO.getSelect()).addJoin(sqlOptionsDTO.getJoin())
                .addFrom(sqlOptionsDTO.getFrom()).addWhere(sqlOptionsDTO.getWhere()));
        System.out.println(sqlResultDO);
        //hiveChange.saveByOrder(sqlResultDO);
    }
    /**
     * 保存
     *
     */
    public void saveByOrder(){
        String sql="insert into a_mart.test values(\"444\",\"text\")";
        List<String> list= Collections.singletonList(sql);
        hiveChange.saveByOrder(list,true);
    }
    /**
     * 定义
     *
     */
    public void ddl(){
        String sql="create table IF NOT EXISTS a_mart.test2019 (id string,name string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE";
        List<String> list= Collections.singletonList(sql);
        Pipeline pipeline = PipelineSingletonUtil.instance;
        PCollection<String> stringPCollection = pipeline.apply(Create.of(list)).setCoder(StringUtf8Coder.of());
        hiveChange.ddl(stringPCollection);

    }
    /**
     * 定义
     *
     */
    public void ddlByOrder(){
        String sql="create table IF NOT EXISTS a_mart.test2019 (id string,name string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE";
        List<String> list= Collections.singletonList(sql);
        hiveChange.ddlByOrder(list,true);
    }
    /**
     * 定义模型
     *
     */
    public void ddlByModel(){
        String json2="{\"optionType\":\"ddl\",\"ddl\":\"create table IF NOT EXISTS a_mart.test2019 (id string,name string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE\"}";
        SQLOptions sqlOptionsDTO = JSON.parseObject(json2, SQLOptions.class);
        SQLResult sqlResultDO = new SQLResult();
        sqlResultDO.ddl(new SQLOperators().addDDL(sqlOptionsDTO.getDdl()));
        hiveChange.ddlByOrder(sqlResultDO);
    }

    /**
     * 批量
     * create table IF NOT EXISTS a_mart.batch2019 (id string,name string,name2 string,name3 string,name4 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
     */
    @Test
    public void batch() throws PropertyVetoException {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        Schema schema = Schema.builder().addStringField("id")
                .addStringField("name").addStringField("name2")
                .addStringField("name3").addStringField("name4").build();

        List<Row> maps=new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            String id="id:"+i;
            String name="name:"+i;
            String name2="name2:"+i;
            String name3="name3:"+i;
            String name4="name4:"+i;
            Row build1 = Row.withSchema(schema).addValue(id).addValue(name).addValue(name2)
                    .addValue(name3).addValue(name4).build();
            maps.add(build1);
        }

        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverName);
        cpds.setJdbcUrl("jdbc:hive2://192.168.20.117:10000/a_mart");
        cpds.setUser("hive");
        cpds.setPassword("hive");

        String sql="insert into a_mart.batch2019(id,name,name2,name3,name4) values(?,?,?,?,?)";
        pipeline.apply(Create.of(maps).withCoder(SchemaCoder.of(schema)))
                .apply(JdbcIO.<Row>write()
                        .withDataSourceConfiguration(
                                JdbcIO.DataSourceConfiguration.create(
                                        cpds))
                        .withStatement(sql)
                        .withPreparedStatementSetter(
                                (element, statement) -> {
                                    List<Object> values = element.getValues();
                                    for (int i = 0; i < values.size(); i++) {
                                        statement.setObject(i+1,values.get(i));
                                    }
                                    int i = statement.executeUpdate();
                                }));

        pipeline.run().waitUntilFinish();
    }


    @Test
    public void batch2() throws PropertyVetoException, SQLException {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverName);
        cpds.setJdbcUrl("jdbc:hive2://192.168.20.117:10000/default");
        cpds.setUser("hive");
        cpds.setPassword("hive");

        Connection connection = cpds.getConnection();
        Statement statement = connection.createStatement();

        statement.executeBatch();
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void insert() throws PropertyVetoException {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driverName);
        cpds.setJdbcUrl("jdbc:hive2://192.168.20.117:10000/default");
        cpds.setUser("hive");
        cpds.setPassword("hive");

        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        String sql="insert into test2019(name,id) values(\"13\",\"1\")";
        String value="test";
        pipeline.apply(Create.of(value)).setCoder(StringUtf8Coder.of())
                .apply(JdbcIO.<String>write()
                        .withDataSourceConfiguration(
                                JdbcIO.DataSourceConfiguration.create(
                                        cpds).withConnectionProperties("hive"))
                        .withStatement(sql)
                        .withPreparedStatementSetter(
                                (element, statement) -> {
                                    int i = statement.executeUpdate();
                                }));

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void insertByMy() throws PropertyVetoException {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:mysql://192.168.20.115:3306/test?useUnicode=true&amp;characterEncoding=UTF-8&amp;zeroDateTimeBehavior=convertToNull");
        cpds.setUser("root");
        cpds.setPassword("root");

        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        String sql="insert into t(name,id) values(\"14\",1)";
        String value="test";
        pipeline.apply(Create.of(value)).setCoder(StringUtf8Coder.of())
                .apply(JdbcIO.<String>write()
                        .withDataSourceConfiguration(
                                JdbcIO.DataSourceConfiguration.create(
                                        cpds).withConnectionProperties("hive"))
                        .withStatement(sql)
                        .withPreparedStatementSetter(
                                (element, statement) -> {

                                }));

        pipeline.run().waitUntilFinish();
    }


    @Test
    public void metadata() throws HCatException {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        Schema schema = Schema.builder().addStringField("id")
                .addStringField("name").addStringField("name2")
                .addStringField("name3").addStringField("name4").build();
        String name1 = schema.getField(0).getName();
        System.out.println(name1);
        String name = schema.getField(0).getType().getTypeName().name();
        System.out.println(name);
        //Schema.TypeName.STRING;

        /**
         *
         * BYTE, // One-byte signed integer.
         *     INT16, // two-byte signed integer.
         *     INT32, // four-byte signed integer.
         *     INT64, // eight-byte signed integer.
         *     DECIMAL, // Arbitrary-precision decimal number
         *     FLOAT,
         *     DOUBLE,
         *     STRING, // String.
         *     DATETIME, // Date and time.
         *     BOOLEAN, // Boolean.
         *     BYTES, // Byte array.
         *     ARRAY,
         *     MAP,
         *     ROW; // The field is itself a nested row.
         *
         */


//
//        List<HCatFieldSchema> columns = new ArrayList<>(5);
//        columns.add(new HCatFieldSchema("id", TypeInfoFactory.stringTypeInfo, ""));
//        columns.add(new HCatFieldSchema("name", TypeInfoFactory.stringTypeInfo, ""));
//        columns.add(new HCatFieldSchema("name2",TypeInfoFactory.stringTypeInfo,""));
//        columns.add(new HCatFieldSchema("name3",TypeInfoFactory.stringTypeInfo,""));
//        columns.add(new HCatFieldSchema("name4",TypeInfoFactory.stringTypeInfo,""));
//        HCatSchema hCatSchema = new HCatSchema(columns);
//
//        List<HCatRecord> expected = new ArrayList<>();
//        HCatRecord record = new DefaultHCatRecord(5);
//        record.set("id",hCatSchema,"newid");
//        record.set("name",hCatSchema,"newname");
//        record.set("name2",hCatSchema,"newname2");
//        record.set("name3",hCatSchema,"newname3");
//        record.set("name4",hCatSchema,"newname4");
//        expected.add(record);
//
//        Map<String, String> configProperties = new HashMap<>();
//        configProperties.put("hive.metastore.uris","thrift://m5.server:9083");
//        //configProperties.put("hive.metastore.local","false");
//
//        pipeline.apply(HCatalogIO.read().withConfigProperties(configProperties).withTable("a_mart.batch2019"));

//        pipeline.apply("record",Create.of(expected))
//                .apply(HCatalogIO.write().withConfigProperties(configProperties).withDatabase("a_mart").withTable("batch2019"));


        pipeline.run().waitUntilFinish();
    }


}
