package com.knowlegene.parent.process;

import com.knowlegene.parent.process.runners.SwapRunners;
import com.knowlegene.parent.process.runners.options.SwapDirectPipelineOptions;

/**
 * java 运行
 * @Classname SwapDirectApplication
 * @Description TODO
 * @Date 2020/7/10 14:52
 * @Created by limeng
 */
public class SwapDirectApplication extends SwapRunners {
    public static void main(String[] args) {
        SwapDirectApplication application=new SwapDirectApplication();
        application.start(SwapDirectPipelineOptions.class,args);
    }

    @Override
    public void setJobStream() {

    }
}
