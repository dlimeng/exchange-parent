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
 * @Date: 2019/10/14 14:03
 */
@RunWith(JUnit4.class)
public class GbaseSwapTest  extends SwapRunners {
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

    /**
     * export
     * gbase-> es
     */
    @Test
    public void testExportES(){
        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String index="kd-test";
        String type="my-type";

        swapOptions.setUrl("jdbc:gbase://192.168.100.1:5258/test");
        swapOptions.setTableName("batch_test");
        swapOptions.setDriverClass("com.gbase.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("gbase");


        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);


    }

    /**
     * export
     * gbase-> es
     */
    @Test
    public void testExportES2(){
        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String index="kd-test";
        String type="my-type";

        swapOptions.setUrl("jdbc:gbase://192.168.100.1:5258/test");
        swapOptions.setTableName("batch_test");
        swapOptions.setDriverClass("com.gbase.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("gbase");
        swapOptions.setDbSQL("select * from batch_test");

        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);


    }
    /**
     * export
     * gbase-> es
     * 嵌套转存es
     */
    @Test
    public void testExportESByNesting(){
        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String index="kd-test";
        String type="my-type";

        swapOptions.setUrl("jdbc:gbase://192.168.100.1:5258/test");
        swapOptions.setTableName("batch_test");
        swapOptions.setDriverClass("com.gbase.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("gbase");
        swapOptions.setDbSQL("select * from batch_test");


//        String[] keys={"id"};
//        String[] columns={"name","createtime","updatetime","desc","age"};
//        String[] nestingValues={"money1","money2","word"};
//        swapOptions.setNestingKeys(keys);
//        swapOptions.setNestingColumns(columns);
//        swapOptions.setNestingValues(nestingValues);

        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);


    }

    /**
     * export
     * gbase-> es
     * 嵌套转存es
     */
    @Test
    public void testExportESByNesting2(){
        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String index="kd-test";
        String type="my-type";

        swapOptions.setUrl("jdbc:gbase://192.168.100.1:5258/test");
        swapOptions.setTableName("batch_test");
        swapOptions.setDriverClass("com.gbase.jdbc.Driver");
        swapOptions.setUsername("root");
        swapOptions.setPassword("gbase");
        swapOptions.setDbSQL("select * from batch_test");


        String[] keys={"id"};
        String[] nestingValues={"money1","money2","word"};
        swapOptions.setNestingKeys(keys);
        swapOptions.setNestingValues(nestingValues);

        swapOptions.setEsAddrs(addrs);
        swapOptions.setEsIndex(index);
        swapOptions.setEsType(type);


    }

}
