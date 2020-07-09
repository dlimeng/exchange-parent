package com.knowlegene.parent.process.route.swap;

import com.knowlegene.parent.process.SwapApplication;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.runners.SwapRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * es
 * @Author: limeng
 * @Date: 2019/9/10 15:24
 */
@RunWith(JUnit4.class)
public class ESSwapTest extends SwapRunners {
    private static SwapApplication application;
    private static SwapOptions swapOptions;
    @Override
    public void setJobStream() {

    }
    @BeforeClass
    public static void beforeClass(){
        application=new SwapApplication();
        swapOptions = new SwapOptions();
    }
    @AfterClass
    public static void  afterClass(){
        application.setSwapOptions(swapOptions);
        application.start();
        application.run();
    }

    /**
     * import
     * es -> hive
     */
    @Test
    public void testImportHive(){
        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String index="kd-test";
        String type="my-type";
        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveTableName("test2019");


    }

    /**
     * 导出 export
     * hive -> es
     */
    @Test
    public void testExportHive(){
        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String index="kd-test";
        String type="my-type";
        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveTableName("test2019");


    }

    /**
     * 导出 export 嵌套
     * hive -> es
     */
    @Test
    public void testExportHive2(){
        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String index="kd-test";
        String type="my-type";
        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveTableName("nestings_test_to_es");

    }


    /**
     * ----------------------------------------------------------------------------------------------------------
     */


    @Test
    public void testMysql(){
        swapOptions.setFromName("es");
        swapOptions.setToName("mysql");

        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String index="kd-test";
        String type="my-type";
        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);

        swapOptions.setUrl("jdbc:mysql://192.168.200.115:3306/kd_test?useSSL=false");
        swapOptions.setTableName("test3");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("root");
    }
    @Test
    public void testOracle(){
        swapOptions.setFromName("es");
        swapOptions.setToName("oracle");

        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String index="kd-test";
        String type="my-type";
        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);


        swapOptions.setUrl("jdbc:oracle:thin:@//192.168.200.25:1521/huaxia");
        swapOptions.setTableName("test1");
        swapOptions.setDriverClass("oracle.jdbc.driver.OracleDriver");
        swapOptions.setUsername("kg");
        swapOptions.setPassword("kg");

    }
    @Test
    public void testHive(){
        swapOptions.setFromName("es");
        swapOptions.setToName("hive");

        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String index="kd-test";
        String type="my-type";
        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);


        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setUsername("hdfs");
        swapOptions.setPassword("hdfs");
        swapOptions.setHiveTableName("pretest");
    }

    @Test
    public void testGbase(){
        swapOptions.setFromName("es");
        swapOptions.setToName("gbase");

        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String index="kd-test";
        String type="my-type";
        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);

        swapOptions.setUrl("jdbc:gbase://192.168.100.1:5258/test");
        swapOptions.setTableName("test3");
        swapOptions.setDriverClass("com.gbase.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("gbase");
    }

    @Test
    public void testFile(){
        swapOptions.setFromName("es");
        swapOptions.setToName("file");

        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String index="kd-test";
        String type="my-type";
        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);

        swapOptions.setFilePath("test.cvs");
        swapOptions.setFieldDelim("#");
    }

    @Test
    public void testES(){
        swapOptions.setFromName("es");
        swapOptions.setToName("es");

        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String index="kd-test";
        String type="my-type";
        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);

//        swapOptions.setEsAddrsFrom();
//        swapOptions.setEsAddrsTo();
        swapOptions.setEsIndexs(new String[]{"",""});
        swapOptions.setEsTypes(new String[]{"",""});

    }

    @Test
    public void testNeo4jNode(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("neo4j");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setUsername("hdfs");
        swapOptions.setPassword("hdfs");
        swapOptions.setHiveTableName("pretest");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");

        //id:ID(Node) name iscp regCap regCapTyp invGrtTyp
        /**
         * 按照模板
         * 模板字段等于插入顺序
         */
        swapOptions.setNeoFormat("id:ID(Node) name age ctime");
    }

    public void testNeo4jNode2(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("neo4j");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setUsername("hdfs");
        swapOptions.setPassword("hdfs");
        swapOptions.setHiveTableName("pretest");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");

        swapOptions.setCypher("CREATE (a:Node {name: {name},age:{age},id:{id},ctime:{ctime}})");
    }

    public void testNeo4jRelate(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("neo4j");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setUsername("hdfs");
        swapOptions.setPassword("hdfs");
        swapOptions.setHiveTableName("pretest");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");

        //id:ID(Node) name iscp regCap regCapTyp invGrtTyp
        /**
         * type 为固定列，标识关系的标签名称
         * isPerson createDate updateDate type title
         */
        swapOptions.setNeoFormat(":START_ID(Node) :END_ID(Node) weight type");
    }

    public void testNeo4jRelate2(){
        swapOptions.setFromName("hive");
        swapOptions.setToName("neo4j");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setUsername("hdfs");
        swapOptions.setPassword("hdfs");
        swapOptions.setHiveTableName("pretest");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");

        //id:ID(Node) name iscp regCap regCapTyp invGrtTyp
        /**
         * type 为固定列，标识关系的标签名称
         * isPerson createDate updateDate type title
         */
        swapOptions.setCypher("MATCH (a:Node),(b:Node) WHERE a.id={startid} AND b.id={endid} \" +\n" +
                "                \" CREATE (a)-[r:Test {weight:{weight}  , type:{type}   }] ->(b) ");
    }




}
