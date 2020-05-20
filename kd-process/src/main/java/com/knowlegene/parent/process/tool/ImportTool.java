package com.knowlegene.parent.process.tool;

import com.knowlegene.parent.process.model.SwapOptions;
import com.knowlegene.parent.process.swap.ImportJobBase;
import lombok.Data;

/**
 * @Author: limeng
 * @Date: 2019/8/20 19:14
 */
@Data
public class ImportTool extends BaseSwapTool{
    public ImportTool() {
    }

    public ImportTool(SwapOptions options) {
        super(options);
    }

    @Override
    public void run(SwapOptions swapOptions) {
        new ImportJobBase(swapOptions).runImport();
    }
}
