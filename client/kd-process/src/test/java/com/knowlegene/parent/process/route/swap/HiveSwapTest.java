package com.knowlegene.parent.process.route.swap;


import com.knowlegene.parent.process.SwapDirectApplication;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.runners.SwapRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @Author: limeng
 * @Date: 2019/9/5 16:12
 */
@RunWith(JUnit4.class)
public class HiveSwapTest extends SwapRunners {
    private static SwapDirectApplication application;
    private static SwapOptions swapOptions;
    @Override
    public void setJobStream() {

    }
    @BeforeClass
    public static void beforeClass(){
        application=new SwapDirectApplication();
        swapOptions = new SwapOptions();
    }
    @AfterClass
    public static void  afterClass(){
        application.setSwapOptions(swapOptions);
        application.start();
    }



    @Test
    public void testMysql(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("mysql");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setHiveUsername("hdfs");
        swapOptions.setHivePassword("hdfs");
        swapOptions.setHiveTableName("pretest");
      //  swapOptions.setHiveSQL("select * from pretest");

        swapOptions.setUrl("jdbc:mysql://192.168.200.115:3306/kd_test?useSSL=false");
        swapOptions.setTableName("test3");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("root");
    }
    @Test
    public void testOracle(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("oracle");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setHiveUsername("hdfs");
        swapOptions.setHivePassword("hdfs");
        swapOptions.setHiveTableName("pretest");
        //  swapOptions.setHiveSQL("select * from pretest");

        swapOptions.setUrl("jdbc:oracle:thin:@//192.168.200.25:1521/huaxia");
        swapOptions.setTableName("test1");
        swapOptions.setDriverClass("oracle.jdbc.driver.OracleDriver");
        swapOptions.setUsername("kg");
        swapOptions.setPassword("kg");

    }
    @Test
    public void testHive(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("hive");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrls(new String[]{"jdbc:hive2://192.168.200.117:10000/linkis_db","jdbc:hive2://192.168.200.117:10000/linkis_db"});
        swapOptions.setHiveUsernames(new String[]{"hdfs","hdfs"});
        swapOptions.setHivePasswords(new String[]{"hdfs","hdfs"});
        swapOptions.setHiveTableNames(new String[]{"test4","test3"});

        swapOptions.setHiveDatabases(new String[]{"linkis_db","linkis_db"});
        //清空目标表数据
        swapOptions.setHiveTableEmpty(true);
        //如果目标表有分区字段，添加上
        swapOptions.setHivePartition("{\"time\":\"11\"}");

        //如果源表有分区，根据表详情中特殊字符晒选出
    }

    @Test
    public void testGbase(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("gbase");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setHiveUsername("hdfs");
        swapOptions.setHivePassword("hdfs");
        swapOptions.setHiveTableName("pretest");
        //  swapOptions.setHiveSQL("select * from pretest");

        swapOptions.setUrl("jdbc:gbase://192.168.100.1:5258/test");
        swapOptions.setTableName("test3");
        swapOptions.setDriverClass("com.gbase.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("gbase");
    }

    @Test
    public void testFile(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("file");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setHiveUsername("hdfs");
        swapOptions.setHivePassword("hdfs");
        swapOptions.setHiveTableName("pretest");
        //  swapOptions.setHiveSQL("select * from pretest");

        swapOptions.setFilePath("test.cvs");
        swapOptions.setFieldDelim("#");
    }

    @Test
    public void testES(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("es");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setHiveUsername("hdfs");
        swapOptions.setHivePassword("hdfs");
        swapOptions.setHiveTableName("pretest");

        String[] addrs=new String[]{"http://192.168.200.101:9200"};
        String index="lmtest";
        String type="_doc";
        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);
        swapOptions.setEsIdFn("id");
    }

    /**
     * 嵌套
     */
    @Test
    public void testESNe(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("es");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setHiveUsername("hdfs");
        swapOptions.setHivePassword("hdfs");
        swapOptions.setHiveTableName("pretest2");

        String[] addrs=new String[]{"http://192.168.200.101:9200"};
        String index="lmtest";
        String type="_doc";
        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);

        String[] key=new String[]{"name"};
        String[] values = new String[]{"tag_name","tag_desc"};
        swapOptions.setNestingKeysName("label_list");
        swapOptions.setNestingKeys(key);
        swapOptions.setNestingValues(values);
    }

    @Test
    public void testNeo4jNode(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("neo4j");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setHiveUsername("hdfs");
        swapOptions.setHivePassword("hdfs");
        swapOptions.setHiveTableName("pretest");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");

        //id:ID(Node) name iscp regCap regCapTyp invGrtTyp
        /**
         * 按照模板
         * 模板字段等于插入顺序
         * 第一个ID固定
         */
        swapOptions.setNeoFormat("id:ID(Node) name age ctime");
    }
    @Test
    public void testNeo4jNode2(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("neo4j");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setHiveUsername("hdfs");
        swapOptions.setHivePassword("hdfs");
        swapOptions.setHiveTableName("pretest");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");

        swapOptions.setCypher("CREATE (a:CNode {targetid: {targetid},entname:{entname},regcap:{regcap},iscp:{iscp}})");
    }

    @Test
    public void testNeo4jRelate(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("neo4j");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setHiveUsername("hdfs");
        swapOptions.setHivePassword("hdfs");
        swapOptions.setHiveTableName("pretest_rel");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");


        /**
         * type 为固定列，标识关系的标签名称
         * 开始 start_id 固定列
         * 结束 end_id 固定列
         */
        swapOptions.setNeoFormat(":START_ID(Node) :END_ID(Node) weight type");
    }

    @Test
    public void testNeo4jRelate2(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("neo4j");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setHiveUsername("hdfs");
        swapOptions.setHivePassword("hdfs");
        swapOptions.setHiveTableName("pretest_rel2");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");


        //id:ID(Node) name iscp regCap regCapTyp invGrtTyp
        /**
         * type 为固定列，标识关系的标签名称
         * isPerson createDate updateDate type title
         */
        swapOptions.setCypher("MATCH (a:Node),(b:Node) WHERE a.id={startid} AND b.id={endid} CREATE (a)-[r:Test {weight:{weight} }] ->(b) ");

        /**
         * MATCH (a:CNode),(b:CNode) WHERE a.targetid={from_id} AND b.targetid={to_id} CREATE (a)-[r:CRelation {from_id:{from_id},fentname:{fentname},ftag:{ftag},fiscp:{fiscp},to_id:{to_id},tentname:{tentname},ttag:{ttag},tiscp:{tiscp},type:{type},groupid:{groupid},topcompanyregcapital:{topcompanyregcapital},topcompanynum:{topcompanynum},regcapitalsum:{regcapitalsum},groupname:{groupname} }] ->(b)
         */
    }


}
