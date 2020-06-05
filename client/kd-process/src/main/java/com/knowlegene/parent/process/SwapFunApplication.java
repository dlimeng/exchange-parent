package com.knowlegene.parent.process;

import com.knowlegene.parent.process.runners.SwapRunners;
import com.knowlegene.parent.process.runners.options.SwapFunPipelineOptions;

/**
 * 数据交换 自定义fun
 * @Author: limeng
 * @Date: 2019/10/15 19:46
 */
public class SwapFunApplication extends SwapRunners {
    public static void main(String[] args) {
        SwapFunApplication application=new SwapFunApplication();

        application.start(SwapFunPipelineOptions.class,args);
        application.run();
    }

    @Override
    public void setJobStream() {
        getImport();
        getExport();
    }
}
