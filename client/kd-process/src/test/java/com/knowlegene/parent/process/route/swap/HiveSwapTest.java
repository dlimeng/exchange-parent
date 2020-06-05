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
 * @Date: 2019/9/5 16:12
 */
@RunWith(JUnit4.class)
public class HiveSwapTest extends SwapRunners {
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
     * 表对表 import mysql->hive hiveserver2
     */
    @Test
    public void testImportMySQL(){
        swapOptions.setUrl("jdbc:mysql://192.168.20.115:3306/test?useSSL=false");
        swapOptions.setTableName("t");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("root");

        //swapOptions.setHiveImport(true);
        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveTableName("test4");

        swapOptions.setImportOptions(true);
    }

    /**
     * mysql sql对hive hiveserver2 字段类型对应好了
     */
    @Test
    public void testImportMySQL2(){
        swapOptions.setUrl("jdbc:mysql://192.168.20.115:3306/test?useSSL=false");
        swapOptions.setTableName("t");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("root");
        swapOptions.setDbSQL("select * from t");


        //swapOptions.setHiveImport(true);
        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveTableName("test2019");

        swapOptions.setImportOptions(true);
    }

    /**
     * mysql 对应 hive metastore直接导入，推荐方法
     */
    @Test
    public void testImportMySQL3(){
       // String[] columns=new String[]{"name","id"};
        swapOptions.setUrl("jdbc:mysql://192.168.20.115:3306/test?useSSL=false");
        swapOptions.setTableName("t");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("root");
        swapOptions.setDbSQL("select * from t");
       // swapOptions.setDbColumn(columns);

        //swapOptions.setHiveImport(true);
        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveDatabase("default");
        swapOptions.setHMetastoreHost("192.168.20.117");
        swapOptions.setHMetastorePort("9083");
        swapOptions.setHiveTableName("test2019");
       // swapOptions.setHiveColumn(columns);

        swapOptions.setImportOptions(true);
    }

    /**
     * mysql 对应 hive metastore直接导入，推荐方法
     */
    @Test
    public void testImportMySQL4(){
        String[] columns=new String[]{"entid","entname","personid"};
        swapOptions.setUrl("jdbc:mysql://40.73.59.12:3306/yuansu_increment?useSSL=false");
        swapOptions.setTableName("company");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("yuansu_increment");
        swapOptions.setPassword("Kboxbr201920192019");
        swapOptions.setDbSQL("select entid,entname,personid from company limit 100");
        swapOptions.setDbColumn(columns);

        //swapOptions.setHiveImport(true);
        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveDatabase("default");
        swapOptions.setHMetastoreHost("192.168.20.117");
        swapOptions.setHMetastorePort("9083");
        swapOptions.setHiveTableName("company_test2");
        swapOptions.setHiveColumn(columns);

        swapOptions.setImportOptions(true);
    }

    /**
     * hive导出cvs 通过metastore导出
     */
    @Test
    public void testExportFile(){
        String[] columns=new String[]{"name","id"};

        //swapOptions.setHiveExport(true);
        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveTableName("test2019");
        swapOptions.setHiveDatabase("default");
        swapOptions.setHMetastoreHost("192.168.20.117");
        swapOptions.setHMetastorePort("9083");
        swapOptions.setHiveColumn(columns);


        swapOptions.setFilePath("test.cvs");

        swapOptions.setExportOptions(true);
    }

    /**
     * hive导出cvs hive通过hiveserver2导出
     */
    @Test
    public void testExportFile2(){

       // String[] columns=new String[]{"name","id"};

        //swapOptions.setHiveExport(true);
        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveTableName("test4");
      //  swapOptions.setHiveColumn(columns);

        swapOptions.setFilePath("test.cvs");

        swapOptions.setExportOptions(true);
    }

}
