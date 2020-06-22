package com.knowlegene.parent.process;

import com.knowlegene.parent.process.runners.SwapRunners;
import com.knowlegene.parent.process.runners.options.SwapFunPipelineOptions;

/**
 * 数据交换 自定义fun
 * @Author: limeng
 * @Date: 2019/10/15 19:46
 */
public class SwapSparkApplication extends SwapRunners {
    public static void main(String[] args) {
        SwapSparkApplication application=new SwapSparkApplication();
        application.start(SwapFunPipelineOptions.class,args);
    }

    @Override
    public void setJobStream() {


    }
}
