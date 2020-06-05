package com.knowlegene.parent.process.runners.options;

import org.apache.beam.sdk.options.Description;

/**
 * 自定义fun
 * @Author: limeng
 * @Date: 2019/10/12 15:21
 */
public interface SwapFunPipelineOptions  extends SwapPipelineOptions{
    /**
     * 嵌套json
     * @return
     */
    @Description("db.嵌套keys")
     String[] getNestingKeys();
     void setNestingKeys(String[] keys);
    @Description("db.普通列")
    String[] getNestingColumns();
     void setNestingColumns(String[] columns);
    @Description("db.嵌套列")
     String[] getNestingValues();
     void setNestingValues(String[] nestings);
}
