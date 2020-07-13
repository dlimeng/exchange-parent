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
 * @Date: 2019/9/30 14:09
 */
@RunWith(JUnit4.class)
public class Neo4jSwapTest   extends SwapRunners {

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
        swapOptions.setFromName("neo4j");
        swapOptions.setToName("mysql");
        //relate
        //node

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");
        //swapOptions.setCypher("match (n:Node) return n");
        swapOptions.setCypher("MATCH p=()-[r:PREL]->() RETURN p LIMIT 25");
        //结点
        //swapOptions.setNeoType("node");
        //关系
        swapOptions.setNeoType("relate");

        swapOptions.setUrl("jdbc:mysql://192.168.200.115:3306/kd_test?useSSL=false");
        swapOptions.setTableName("source_rel1");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("root");
    }


    @Test
    public void testOracle(){
        swapOptions.setFromName("neo4j");
        swapOptions.setToName("oracle");


        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");
        //swapOptions.setCypher("match (n:Node) return n");
        swapOptions.setCypher("MATCH p=()-[r:PREL]->() RETURN p LIMIT 25");
        //结点
        //swapOptions.setNeoType("node");
        //关系
        swapOptions.setNeoType("relate");

        swapOptions.setUrl("jdbc:oracle:thin:@//192.168.200.25:1521/huaxia");
        swapOptions.setTableName("test1");
        swapOptions.setDriverClass("oracle.jdbc.driver.OracleDriver");
        swapOptions.setUsername("kg");
        swapOptions.setPassword("kg");
    }
    @Test
    public void testHive(){
        swapOptions.setFromName("neo4j");
        swapOptions.setToName("hive");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");
        //swapOptions.setCypher("match (n:Node) return n");
        swapOptions.setCypher("MATCH p=()-[r:PREL]->() RETURN p LIMIT 25");
        //结点
        //swapOptions.setNeoType("node");
        //关系
        swapOptions.setNeoType("relate");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setHiveUsername("hdfs");
        swapOptions.setHivePassword("hdfs");
        swapOptions.setHiveTableName("pretest");
        swapOptions.setHiveTableEmpty(true);
    }

    @Test
    public void testGbase(){
        swapOptions.setFromName("neo4j");
        swapOptions.setToName("gbase");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");
        //swapOptions.setCypher("match (n:Node) return n");
        swapOptions.setCypher("MATCH p=()-[r:PREL]->() RETURN p LIMIT 25");
        //结点
        //swapOptions.setNeoType("node");
        //关系
        swapOptions.setNeoType("relate");

        swapOptions.setUrl("jdbc:gbase://192.168.100.1:5258/test");
        swapOptions.setTableName("soure1");
        swapOptions.setDriverClass("com.gbase.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("gbase");
    }

    @Test
    public void testFile(){
        swapOptions.setFromName("neo4j");
        swapOptions.setToName("file");


        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");
        //swapOptions.setCypher("match (n:Node) return n");
        swapOptions.setCypher("MATCH p=()-[r:PREL]->() RETURN p LIMIT 25");
        //结点
        //swapOptions.setNeoType("node");
        //关系
        swapOptions.setNeoType("relate");

        swapOptions.setFilePath("test.cvs");
        swapOptions.setFieldDelim("#");
    }

    @Test
    public void testES(){
        swapOptions.setFromName("neo4j");
        swapOptions.setToName("es");


        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");
        //swapOptions.setCypher("match (n:Node) return n");
        swapOptions.setCypher("MATCH p=()-[r:PREL]->() RETURN p LIMIT 25");
        //结点
        //swapOptions.setNeoType("node");
        //关系
        swapOptions.setNeoType("relate");

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
        swapOptions.setFromName("neo4j");
        swapOptions.setToName("es");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");
        //swapOptions.setCypher("match (n:Node) return n");
        swapOptions.setCypher("MATCH p=()-[r:PREL]->() RETURN p LIMIT 25");
        //结点
        //swapOptions.setNeoType("node");
        //关系
        swapOptions.setNeoType("relate");

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
        swapOptions.setFromName("neo4j");
        swapOptions.setToName("neo4j");




        swapOptions.setNeoUrls(new String[]{"bolt://localhost:7687","bolt://localhost:7687"});
        swapOptions.setNeoUsernames(new String[]{"neo4j","neo4j"});
        swapOptions.setNeoPasswords(new String[]{"limeng","limeng"});

        swapOptions.setNeoType("node");
        swapOptions.setCypher("match (n:Node) return n");

        //id:ID(Node) name iscp regCap regCapTyp invGrtTyp
        /**
         * 按照模板
         * 模板字段等于插入顺序
         * 第一个ID固定
         */
        swapOptions.setNeoFormat("id:ID(Node2) name age ctime");
    }
    @Test
    public void testNeo4jNode2(){
        swapOptions.setFromName("neo4j");
        swapOptions.setToName("neo4j");

        swapOptions.setNeoUrls(new String[]{"bolt://localhost:7687","bolt://localhost:7687"});
        swapOptions.setNeoUsernames(new String[]{"neo4j","neo4j"});
        swapOptions.setNeoPasswords(new String[]{"limeng","limeng"});

        swapOptions.setNeoType("node");
        swapOptions.setCyphers(new String[]{"match (n:Node) return n","CREATE (a:Node {name: {name},age:{age},id:{id},ctime:{ctime}})"});

    }

    @Test
    public void testNeo4jRelate(){
        swapOptions.setFromName("neo4j");
        swapOptions.setToName("neo4j");

        swapOptions.setNeoUrls(new String[]{"bolt://localhost:7687","bolt://localhost:7687"});
        swapOptions.setNeoUsernames(new String[]{"neo4j","neo4j"});
        swapOptions.setNeoPasswords(new String[]{"limeng","limeng"});

        swapOptions.setNeoType("relate");
        swapOptions.setCypher("MATCH p=()-[r:PREL]->() RETURN p LIMIT 25");


        /**
         * type 为固定列，标识关系的标签名称
         * 开始 start_id 固定列
         * 结束 end_id 固定列
         */
        swapOptions.setNeoFormat(":START_ID(Node2) :END_ID(Node) weight type");
    }

    @Test
    public void testNeo4jRelate2(){
        swapOptions.setFromName("mysql");
        swapOptions.setToName("neo4j");

        swapOptions.setUrl("jdbc:mysql://192.168.200.115:3306/kd_test?useSSL=false");
        swapOptions.setTableName("source_rel1");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("root");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");


        swapOptions.setNeoUrls(new String[]{"bolt://localhost:7687","bolt://localhost:7687"});
        swapOptions.setNeoUsernames(new String[]{"neo4j","neo4j"});
        swapOptions.setNeoPasswords(new String[]{"limeng","limeng"});

        swapOptions.setNeoType("relate");
        swapOptions.setCyphers(new String[]{"MATCH p=()-[r:PREL]->() RETURN p LIMIT 25","MATCH (a:Node),(b:Node) WHERE a.id={start_id} AND b.id={end_id} CREATE (a)-[r:Test {weight:{weight} ,type:{type}}] ->(b)"});

        //id:ID(Node) name iscp regCap regCapTyp invGrtTyp
        /**
         * type 为固定列，标识关系的标签名称
         * isPerson createDate updateDate type title
         */

    }



}
