package com.knowlegene.parent.process;

import com.knowlegene.parent.process.runners.SwapRunners;
import com.knowlegene.parent.process.runners.options.SwapFlinkPipelineOptions;

/**
 * flink
 * @Classname SwapFlinkApplication
 * @Description TODO
 * @Date 2020/7/9 17:24
 * @Created by limeng
 */
public class SwapFlinkApplication extends SwapRunners {
    public static void main(String[] args) {
        SwapFlinkApplication application=new SwapFlinkApplication();
        application.start(SwapFlinkPipelineOptions.class,args);
    }

    @Override
    public void setJobStream() {

    }
}
