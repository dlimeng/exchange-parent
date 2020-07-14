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
 * @Date: 2019/9/5 16:11
 */
@RunWith(JUnit4.class)
public class OracleSwapTest extends SwapRunners {
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
        swapOptions.setFromName("oracle");
        swapOptions.setToName("mysql");


        swapOptions.setDriverClasss(new String[]{"oracle.jdbc.driver.OracleDriver","com.mysql.jdbc.Driver"});
        swapOptions.setUrls(new String[]{"jdbc:oracle:thin:@//192.168.200.25:1521/huaxia","jdbc:mysql://192.168.200.115:3306/kd_test?useSSL=false"});
        swapOptions.setUsernames(new String[]{"kg","root"});
        swapOptions.setPasswords(new String[]{"kg","root"});
        swapOptions.setTableNames(new String[]{"soure1","test3"});
    }
    @Test
    public void testOracle(){
        swapOptions.setFromName("oracle");
        swapOptions.setToName("oracle");

        swapOptions.setDriverClasss(new String[]{"oracle.jdbc.driver.OracleDriver","oracle.jdbc.driver.OracleDriver"});
        swapOptions.setUrls(new String[]{"jdbc:oracle:thin:@//192.168.200.25:1521/huaxia","jdbc:oracle:thin:@//192.168.200.25:1521/huaxia"});
        swapOptions.setUsernames(new String[]{"kg","kg"});
        swapOptions.setPasswords(new String[]{"kg","kg"});
        swapOptions.setTableNames(new String[]{"soure1","test1"});

    }


    @Test
    public void testGbase(){
        swapOptions.setFromName("oracle");
        swapOptions.setToName("gbase");


        swapOptions.setDriverClasss(new String[]{"oracle.jdbc.driver.OracleDriver","com.gbase.jdbc.Driver"});
        swapOptions.setUrls(new String[]{"jdbc:oracle:thin:@//192.168.200.25:1521/huaxia","jdbc:gbase://192.168.100.1:5258/test"});
        swapOptions.setUsernames(new String[]{"kg","root"});
        swapOptions.setPasswords(new String[]{"kg","gbase"});
        swapOptions.setTableNames(new String[]{"soure1","test3"});
    }

    @Test
    public void testHive(){
        swapOptions.setFromName("oracle");
        swapOptions.setToName("hive");


        swapOptions.setUrl("jdbc:oracle:thin:@//192.168.200.25:1521/huaxia");
        swapOptions.setTableName("soure1");
        swapOptions.setDriverClass("oracle.jdbc.driver.OracleDriver");
        swapOptions.setUsername("kg");
        swapOptions.setPassword("kg");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setHiveUsername("hdfs");
        swapOptions.setHivePassword("hdfs");
        swapOptions.setHiveTableName("pretest");
        swapOptions.setHiveTableEmpty(true);
    }



    @Test
    public void testFile(){
        swapOptions.setFromName("oracle");
        swapOptions.setToName("file");

        swapOptions.setUrl("jdbc:oracle:thin:@//192.168.200.25:1521/huaxia");
        swapOptions.setTableName("soure1");
        swapOptions.setDriverClass("oracle.jdbc.driver.OracleDriver");
        swapOptions.setUsername("kg");
        swapOptions.setPassword("kg");

        swapOptions.setFilePath("test.cvs");
        swapOptions.setFieldDelim("#");
    }

    @Test
    public void testES(){
        swapOptions.setFromName("oracle");
        swapOptions.setToName("es");

        swapOptions.setUrl("jdbc:oracle:thin:@//192.168.200.25:1521/huaxia");
        swapOptions.setTableName("soure1");
        swapOptions.setDriverClass("oracle.jdbc.driver.OracleDriver");
        swapOptions.setUsername("kg");
        swapOptions.setPassword("kg");

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
        swapOptions.setFromName("oracle");
        swapOptions.setToName("es");

        swapOptions.setUrl("jdbc:oracle:thin:@//192.168.200.25:1521/huaxia");
        swapOptions.setTableName("soure2");
        swapOptions.setDriverClass("oracle.jdbc.driver.OracleDriver");
        swapOptions.setUsername("kg");
        swapOptions.setPassword("kg");

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
        swapOptions.setFromName("oracle");
        swapOptions.setToName("neo4j");

        swapOptions.setUrl("jdbc:oracle:thin:@//192.168.200.25:1521/huaxia");
        swapOptions.setTableName("soure1");
        swapOptions.setDriverClass("oracle.jdbc.driver.OracleDriver");
        swapOptions.setUsername("kg");
        swapOptions.setPassword("kg");


        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");

        //id:ID(Node) name iscp regCap regCapTyp invGrtTyp
        /**
         * 按照模板
         * 模板字段等于插入顺序
         * 第一个ID固定
         * node
         */
        swapOptions.setNeoFormat("id:ID(Node) name age ctime");
    }
    @Test
    public void testNeo4jNode2(){
        swapOptions.setFromName("oracle");
        swapOptions.setToName("neo4j");

        swapOptions.setUrl("jdbc:oracle:thin:@//192.168.200.25:1521/huaxia");
        swapOptions.setTableName("soure1");
        swapOptions.setDriverClass("oracle.jdbc.driver.OracleDriver");
        swapOptions.setUsername("kg");
        swapOptions.setPassword("kg");


        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");

        swapOptions.setCypher("CREATE (a:Node {name: {name},age:{age},id:{id},ctime:{ctime}})");
    }

    @Test
    public void testNeo4jRelate(){
        swapOptions.setFromName("oracle");
        swapOptions.setToName("neo4j");

        swapOptions.setUrl("jdbc:oracle:thin:@//192.168.200.25:1521/huaxia");
        swapOptions.setTableName("SOURCE_REL1");
        swapOptions.setDriverClass("oracle.jdbc.driver.OracleDriver");
        swapOptions.setUsername("kg");
        swapOptions.setPassword("kg");

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
        swapOptions.setFromName("oracle");
        swapOptions.setToName("neo4j");

        swapOptions.setUrl("jdbc:oracle:thin:@//192.168.200.25:1521/huaxia");
        swapOptions.setTableName("SOURCE_REL1");
        swapOptions.setDriverClass("oracle.jdbc.driver.OracleDriver");
        swapOptions.setUsername("kg");
        swapOptions.setPassword("kg");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");

        //id:ID(Node) name iscp regCap regCapTyp invGrtTyp
        /**
         * type 为固定列，标识关系的标签名称
         * isPerson createDate updateDate type title
         */
        swapOptions.setCypher("MATCH (a:Node),(b:Node) WHERE a.id={start_id} AND b.id={end_id} CREATE (a)-[r:Test {weight:{weight} ,type:{type}}] ->(b) ");
    }

}
