package com.knowlegene.parent.process;

import com.knowlegene.parent.process.model.SwapOptions;
import com.knowlegene.parent.process.runners.SwapRunners;

/**
 * @Author: limeng
 * @Date: 2019/9/25 14:03
 */
public class SwapAndSparkApplication extends SwapRunners {
    @Override
    public void setJobStream() {
        getImport();
        getExport();
    }


    public static void main(String[] args) {
        SwapAndSparkApplication swapApplicationTest = new SwapAndSparkApplication();
        SwapOptions swapOptions = new SwapOptions();

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

        swapOptions.setExportOptions(true);

        swapApplicationTest.setSwapOptions(swapOptions);
        swapApplicationTest.start(args);
        swapApplicationTest.run();
    }
}
