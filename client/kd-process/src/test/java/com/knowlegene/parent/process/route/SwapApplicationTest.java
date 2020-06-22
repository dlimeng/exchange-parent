package com.knowlegene.parent.process.route;

import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.runners.SwapRunners;


/**
 * 数据交换
 * @Author: limeng
 * @Date: 2019/9/2 14:52
 */
public class SwapApplicationTest extends SwapRunners {
    @Override
    public void setJobStream() {

    }
    public static void main(String[] args) {
//        SwapApplicationTest swapApplicationTest = new SwapApplicationTest();
//
//        swapApplicationTest.start(SwapFunPipelineOptions.class,args);
//        swapApplicationTest.run();
        save(args);
    }

    public static void  nestings(String[] args){
//        System.setProperty("java.io.tmpdir","D:\\tmp");
//        SwapAndSparkApplication swapApplicationTest = new SwapAndSparkApplication();
//        SwapOptions swapOptions = new SwapOptions();
//
//        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
//        String index="kd-test";
//        String type="my-type";
//
//        swapOptions.setUrl("jdbc:gbase://192.168.100.1:5258/test");
//        swapOptions.setTableName("batch_test");
//        swapOptions.setDriverClass("com.gbase.jdbc.Driver");
//        swapOptions.setUsername("root");
//        swapOptions.setPassword("gbase");
//        swapOptions.setDbSQL("select * from batch_test");
//
//
//        String[] keys={"id"};
//        String[] nestingValues={"money1","money2","word"};
//        swapOptions.setNestingKeys(keys);
//        swapOptions.setNestingValues(nestingValues);
//
//        swapOptions.setEsAddrs(addrs);
//        swapOptions.setEsIndex(index);
//        swapOptions.setEsType(type);
//
//        swapOptions.setExportOptions(true);
//
//        swapApplicationTest.setSwapOptions(swapOptions);
//        swapApplicationTest.start(args);
//        swapApplicationTest.run();
    }


    public static void  save(String[] args){
//        System.setProperty("java.io.tmpdir","D:\\tmp");
//        SwapAndSparkApplication swapApplicationTest = new SwapAndSparkApplication();
//        SwapOptions swapOptions = new SwapOptions();
//
//        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
//        String index="kd-test";
//        String type="my-type";
//
//        swapOptions.setUrl("jdbc:gbase://192.168.100.1:5258/test");
//        swapOptions.setTableName("batch_test");
//        swapOptions.setDriverClass("com.gbase.jdbc.Driver");
//        swapOptions.setUsername("root");
//        swapOptions.setPassword("gbase");
//        swapOptions.setDbSQL("select * from batch_test");
//
//
//
//        swapOptions.setEsAddrs(addrs);
//        swapOptions.setEsIndex(index);
//        swapOptions.setEsType(type);
//
//        swapOptions.setExportOptions(true);
//
//        swapApplicationTest.setSwapOptions(swapOptions);
//        swapApplicationTest.start(args);
//        swapApplicationTest.run();
    }

    public static void  mysqlToHive(String[] args){
//        System.setProperty("java.io.tmpdir","D:\\tmp");
//        SwapAndSparkApplication swapApplicationTest = new SwapAndSparkApplication();
//        SwapOptions swapOptions = new SwapOptions();
//        String[] columns=new String[]{"entid","entname","personid"};
//        swapOptions.setUrl("jdbc:mysql://40.73.59.12:3306/yuansu_increment?useSSL=false");
//        swapOptions.setTableName("company");
//        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
//        swapOptions.setUsername("yuansu_increment");
//        swapOptions.setPassword("Kboxbr201920192019");
//        swapOptions.setDbSQL("select entid,entname,personid from company limit 100");
//        swapOptions.setDbColumn(columns);
//
//        //swapOptions.setHiveImport(true);
//        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
//        //jdbc:hive2://192.168.20.117:10000/default
//        swapOptions.setHiveUrl("jdbc:hive2://nlp.server:2181,m5.server:2181,m4.server:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-hive2");
//        swapOptions.setHiveUsername("hive");
//        swapOptions.setHivePassword("hive");
//        swapOptions.setHiveDatabase("default");
//        swapOptions.setHMetastoreHost("m5.server");
//        swapOptions.setHMetastorePort("9083");
//        swapOptions.setHiveTableName("company_test2");
//        swapOptions.setHiveColumn(columns);
//
//        swapOptions.setImportOptions(true);
//
//        swapApplicationTest.setSwapOptions(swapOptions);
//        swapApplicationTest.start(args);
//        swapApplicationTest.run();
    }

}
