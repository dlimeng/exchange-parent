package com.knowlegene.parent.process.tool;

import com.knowlegene.parent.process.model.SwapOptions;
import com.knowlegene.parent.process.swap.ExportJobBase;
import com.knowlegene.parent.process.swap.ImportJobBase;

/**
 * @Author: limeng
 * @Date: 2019/8/20 19:14
 */
public class ExportTool extends BaseSwapTool{

    public ExportTool() {
    }

    public ExportTool(SwapOptions options) {
        super(options);
    }

    @Override
    public void run(SwapOptions swapOptions) {
        new ExportJobBase(swapOptions).runExport();
    }
}
