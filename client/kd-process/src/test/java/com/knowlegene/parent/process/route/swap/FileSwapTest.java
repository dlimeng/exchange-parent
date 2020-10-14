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
 * @Date: 2019/9/5 16:10
 */
@RunWith(JUnit4.class)
public class FileSwapTest  extends SwapRunners {
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
        application.run();
    }

    @Test
    public void testMysql(){
        swapOptions.setFromName("file");
        swapOptions.setToName("mysql");

        swapOptions.setFieldTitle(new String[]{"id","name","age","ctime"});
        swapOptions.setFieldDelim("#");
        swapOptions.setFilePath("D:\\workspace\\open_source\\exchange-parent\\client\\kd-process\\test-00000-of-00003.cvs");

        swapOptions.setUrl("jdbc:mysql://192.168.200.115:3306/kd_test?useSSL=false");
        swapOptions.setTableName("test3_copy");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("root");
    }
    @Test
    public void testOracle(){
        swapOptions.setFromName("file");
        swapOptions.setToName("oracle");

        swapOptions.setFieldTitle(new String[]{"id","name","age","ctime"});
        swapOptions.setFieldDelim("#");
        swapOptions.setFilePath("D:\\workspace\\open_source\\exchange-parent\\client\\kd-process\\test-00000-of-00003.cvs");

        swapOptions.setUrl("jdbc:oracle:thin:@//192.168.200.25:1521/huaxia");
        swapOptions.setTableName("test1");
        swapOptions.setDriverClass("oracle.jdbc.driver.OracleDriver");
        swapOptions.setUsername("kg");
        swapOptions.setPassword("kg");

    }
    @Test
    public void testHive(){
        swapOptions.setFromName("file");
        swapOptions.setToName("hive");

        swapOptions.setFieldTitle(new String[]{"id","name","age","ctime"});
        swapOptions.setFieldDelim("#");
        swapOptions.setFilePath("D:\\workspace\\open_source\\exchange-parent\\client\\kd-process\\test-00000-of-00003.cvs");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.200.117:10000/linkis_db");
        swapOptions.setHiveUsername("hdfs");
        swapOptions.setHivePassword("hdfs");
        swapOptions.setHiveTableName("pretest");
    }

    @Test
    public void testGbase(){
        swapOptions.setFromName("file");
        swapOptions.setToName("gbase");

        swapOptions.setFieldTitle(new String[]{"id","name","age","ctime"});
        swapOptions.setFieldDelim("#");
        swapOptions.setFilePath("D:\\workspace\\open_source\\exchange-parent\\client\\kd-process\\test-00000-of-00003.cvs");

        swapOptions.setUrl("jdbc:gbase://192.168.100.1:5258/test");
        swapOptions.setTableName("test3");
        swapOptions.setDriverClass("com.gbase.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("gbase");
    }

    @Test
    public void testFile(){
        swapOptions.setFromName("file");
        swapOptions.setToName("file");

        swapOptions.setFieldTitle(new String[]{"id","name","age","ctime"});
        swapOptions.setFieldDelims(new String[]{"#","#"});
        swapOptions.setFilePaths(new String[]{"D:\\workspace\\open_source\\exchange-parent\\client\\kd-process\\test-00000-of-00003.cvs","ccc.cvs"});


    }

    @Test
    public void testES(){
        swapOptions.setFromName("file");
        swapOptions.setToName("es");

        swapOptions.setFieldTitle(new String[]{"id","name","age","ctime"});
        swapOptions.setFieldDelim("#");
        swapOptions.setFilePath("D:\\workspace\\open_source\\exchange-parent\\client\\kd-process\\test-00000-of-00003.cvs");

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
        swapOptions.setFromName("file");
        swapOptions.setToName("es");

        swapOptions.setFieldTitle(new String[]{"id","name","age","ctime"});
        swapOptions.setFieldDelim("#");
        swapOptions.setFilePath("D:\\workspace\\open_source\\exchange-parent\\client\\kd-process\\test-00000-of-00003.cvs");

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
        swapOptions.setFromName("file");
        swapOptions.setToName("neo4j");

        swapOptions.setFieldTitle(new String[]{"id","name","age","ctime"});
        swapOptions.setFieldDelim("#");
        swapOptions.setFilePath("D:\\workspace\\open_source\\exchange-parent\\client\\kd-process\\test-00000-of-00003.cvs");

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
        swapOptions.setFromName("file");
        swapOptions.setToName("neo4j");

        swapOptions.setFieldTitle(new String[]{"id","name","age","ctime"});
        swapOptions.setFieldDelim("#");
        swapOptions.setFilePath("D:\\workspace\\open_source\\exchange-parent\\client\\kd-process\\test-00000-of-00003.cvs");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");

        swapOptions.setCypher("CREATE (a:Node {name: {name},age:{age},id:{id},ctime:{ctime}})");
    }

    @Test
    public void testNeo4jRelate(){
        swapOptions.setFromName("file");
        swapOptions.setToName("neo4j");

        swapOptions.setFieldTitle(new String[]{"start_id","end_id","weight","type"});
        swapOptions.setFieldDelim("\t");
        swapOptions.setFilePath("D:\\workspace\\open_source\\exchange-parent\\client\\kd-process\\test-00000-of-00003.cvs");

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
        swapOptions.setFromName("file");
        swapOptions.setToName("neo4j");

        swapOptions.setFieldTitle(new String[]{"start_id","end_id","weight","type"});
        swapOptions.setFieldDelim("#");
        swapOptions.setFilePath("D:\\workspace\\open_source\\exchange-parent\\client\\kd-process\\test-00000-of-00003.cvs");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");

        //id:ID(Node) name iscp regCap regCapTyp invGrtTyp
        /**
         * type 为固定列，标识关系的标签名称
         * isPerson createDate updateDate type title
         */
        swapOptions.setCypher("MATCH (a:Node),(b:Node) WHERE a.id={start_id} AND b.id={end_id} CREATE (a)-[r:Test {weight:{weight} }] ->(b) ");
    }


    @Test
    public void testNeo4jRelate3(){
        swapOptions.setFromName("file");
        swapOptions.setToName("neo4j");

        swapOptions.setFieldTitle(new String[]{"from_id","fentname","ftag","fiscp","to_id","tentname","ttag","tiscp","type","groupid","topcompanyregcapital","topcompanynum","regcapitalsum","groupname"});
        swapOptions.setFieldDelim("\t");
        swapOptions.setFilePath("D:\\result.txt");

        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");

        //id:ID(Node) name iscp regCap regCapTyp invGrtTyp
        /**
         * type 为固定列，标识关系的标签名称
         * isPerson createDate updateDate type title
         */
        swapOptions.setCypher("MATCH (a:CNode),(b:CNode) WHERE a.targetid={from_id} AND b.targetid={to_id} CREATE (a)-[r:CRelation {from_id:{from_id},fentname:{fentname},ftag:{ftag},fiscp:{fiscp},to_id:{to_id},tentname:{tentname},ttag:{ttag},tiscp:{tiscp},type:{type},groupid:{groupid},topcompanyregcapital:{topcompanyregcapital},topcompanynum:{topcompanynum},regcapitalsum:{regcapitalsum},groupname:{groupname} }] ->(b)");
    }

}
