package com.knowlegene.parent.process.route.swap;

import com.knowlegene.parent.process.SwapApplication;
import com.knowlegene.parent.process.model.SwapOptions;
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

    private static SwapApplication application;
    private static SwapOptions swapOptions;
    @Override
    public void setJobStream() {
        getImport();
        getExport();
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
     * 导出 export
     * hive -> neo4j
     * save
     */
    @Test
    public void testExportSave(){

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveTableName("neo4j_test");


        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");
        swapOptions.setNeoFormat("id:ID(Node) name iscp regCap regCapTyp invGrtTyp");

        swapOptions.setExportOptions(true);
    }

    @Test
    public void testExportSave2(){
        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveTableName("neo4j_test");


        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");
        /**
         * 标签名称Node 语句字段名称跟hive表列名一致
         */
        swapOptions.setCypher("CREATE (a:Node {id: {id}, name: {name},iscp:{iscp},regcap:{regcap},regcaptyp:{regcaptyp},invgrttyp:{invgrttyp} })");

        swapOptions.setExportOptions(true);
    }

    /**
     * 导出 export
     * hive -> neo4j
     * relate
     */
    @Test
    public void testExportRelate(){

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveTableName("neo4j_relate");


        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");
        /**
         * type 为固定列，标识关系的标签名称
         */
        swapOptions.setNeoFormat(":START_ID(Node) :END_ID(Node) weight isPerson createDate updateDate type title");

        swapOptions.setExportOptions(true);
    }

    @Test
    public void testExportRelate2(){
        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveTableName("neo4j_relate");


        swapOptions.setNeoUrl("bolt://localhost:7687");
        swapOptions.setNeoUsername("neo4j");
        swapOptions.setNeoPassword("limeng");
        /**
         * 节点 标签名称 Node
         * 连接 标签名称Test
         * 语句字段名称跟hive表列名一致
         */
        swapOptions.setCypher("MATCH (a:Node),(b:Node) WHERE a.id={start_id} AND b.id={end_id} " +
                " CREATE (a)-[r:Test {weight:{weight} ,isperson:{isperson} ,createdate:{createdate} ,updatedate:{updatedate} , type:{type} , title:{title}  }] ->(b) ");


        swapOptions.setExportOptions(true);
    }


}
