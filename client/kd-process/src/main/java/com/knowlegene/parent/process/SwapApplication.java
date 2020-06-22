package com.knowlegene.parent.process;



import com.knowlegene.parent.process.runners.SwapRunners;

/**
 * 数据交换 基础
 * @Author: limeng
 *
 * @Date: 2019/7/15 10:06
 */
public class SwapApplication extends SwapRunners {
    public static void main(String[] args) {
        SwapApplication application=new SwapApplication();
        application.start(args);
    }
    @Override
    public void setJobStream() {

    }
}
