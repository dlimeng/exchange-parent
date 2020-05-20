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
 * @Date: 2019/9/5 16:11
 */
@RunWith(JUnit4.class)
public class MySQLSwapTest  extends SwapRunners {
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
        //更改引擎
        application.setSwapOptions(swapOptions);
        application.start();
        application.run();
    }

    /**
     * 表对表 import mysql->hive hiveserver2
     */
    @Test
    public void testImportHive(){
        swapOptions.setUrl("jdbc:mysql://192.168.20.115:3306/test?useSSL=false");
        swapOptions.setTableName("t");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("root");


        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveTableName("test2019");

        swapOptions.setImportOptions(true);
    }

    /**
     * mysql -> es
     */
    @Test
    public void testExportES(){
        //String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String[] addrs=new String[]{"http://192.168.200.18:9213"};
        String index="kd-test";
        String type="_doc";


        swapOptions.setUrl("jdbc:mysql://192.168.200.115:3306/kd_test?useSSL=false");
        swapOptions.setTableName("test");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("root");
        swapOptions.setDbColumn(new String[]{"name","id"});

        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);
        swapOptions.setEsIdFn("name");

        swapOptions.setExportOptions(true);
    }

    /**
     * mysql -> es
     * sql语句
     */
    @Test
    public void testExportES2(){
        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
       // String[] addrs=new String[]{"http://192.168.20.118:9200"};
        String index="kd-test2";
        String type="_doc";


        swapOptions.setUrl("jdbc:mysql://192.168.20.115:3306/kd_test?useSSL=false");
        swapOptions.setTableName("aa");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("root");
        swapOptions.setDbSQL("select * from aa limit 10");

        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);
        swapOptions.setEsIdFn("itcode");

        swapOptions.setExportOptions(true);
    }

    /**
     * mysql -> es
     * 嵌套转存es
     */
    @Test
    public void testExportES3(){
//        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
//        String index="kd-test";
//        String type="my-type";

        String[] addrs=new String[]{"http://192.168.20.118:9200"};
        String index="kd-test";
        String type="_doc";

        swapOptions.setUrl("jdbc:mysql://192.168.20.115:3306/test?useSSL=false");
        swapOptions.setTableName("t");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("root");



        String[] keys={"name"};
        String[] columns={"id","time"};
        String[] nestingValues={"desc"};
        swapOptions.setNestingKeys(keys);
        swapOptions.setNestingColumns(columns);
        swapOptions.setNestingValues(nestingValues);

        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);

        swapOptions.setExportOptions(true);
    }


    /**
     * mysql -> es
     * 嵌套转存es
     */
    @Test
    public void testExportES4(){
        String[] addrs=new String[]{"http://192.168.20.118:9200"};
        String index="kd-test";
        String type="_doc";


        swapOptions.setUrl("jdbc:mysql://192.168.20.115:3306/test?useSSL=false");
        swapOptions.setTableName("t");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("root");

        String[] keys={"name"};
        String[] nestingValues={"desc"};
        swapOptions.setNestingKeys(keys);
        swapOptions.setNestingValues(nestingValues);

        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);

        swapOptions.setExportOptions(true);
    }

}
