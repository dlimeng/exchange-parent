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
public class OracleSwapTest extends SwapRunners {
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
    public void testImportHive(){
        swapOptions.setUrl("jdbc:oracle:thin:@//192.168.200.25:1521/huaxia");
        swapOptions.setTableName("test1");
        swapOptions.setDriverClass("oracle.jdbc.driver.OracleDriver");
        swapOptions.setUsername("kg");
        swapOptions.setPassword("kg");

        //swapOptions.setHiveImport(true);
        swapOptions.setHiveClass("org.apache.hive.jdbc.HiveDriver");
        swapOptions.setHiveUrl("jdbc:hive2://192.168.20.117:10000/default");
        swapOptions.setHiveUsername("hive");
        swapOptions.setHivePassword("hive");
        swapOptions.setHiveTableName("test3");

        swapOptions.setImportOptions(true);
    }
}
