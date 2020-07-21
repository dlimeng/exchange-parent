package com.knowlegene.parent.process;

import com.knowlegene.parent.process.runners.SwapRunners;
import com.knowlegene.parent.process.runners.options.SwapJavaPipelineOptions;


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
        application.start(SwapJavaPipelineOptions.class,args);
    }

    @Override
    public void setJobStream() {

    }
}
