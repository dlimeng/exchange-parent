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

        swapOptions.setImportOptions(true);
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

        swapOptions.setExportOptions(true);
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
        swapOptions.setExportOptions(true);
    }
}
