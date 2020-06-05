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
 * @Date: 2019/9/5 16:10
 */
@RunWith(JUnit4.class)
public class FileSwapTest  extends SwapRunners {
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
    @Test
    public void testImportMySQL(){
        swapOptions.setFilePath("D:\\工具\\workspace_new\\kd-parent\\kd-process\\src\\main\\resources\\template\\test-00000-of-00005.cvs");
        swapOptions.setFieldDelim("\t");

        swapOptions.setUrl("jdbc:mysql://192.168.20.115:3306/test?useSSL=false");
        swapOptions.setTableName("t");
        swapOptions.setDriverClass("com.mysql.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("root");

        swapOptions.setImportOptions(true);
    }
    @Test
    public void testImportHive(){
        swapOptions.setFilePath("D:\\工具\\workspace_new\\kd-parent\\kd-process\\src\\main\\resources\\template\\test-00000-of-00005.cvs");
        swapOptions.setFieldDelim("\t");

        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveTableName("test2019");
//        swapOptions.setHiveDatabase("default");
//        swapOptions.setHMetastoreHost("192.168.20.117");
//        swapOptions.setHMetastorePort("9083");
//        swapOptions.setHiveTableName("test2019");

        swapOptions.setImportOptions(true);
    }




}
