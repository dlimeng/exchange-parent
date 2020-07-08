package com.knowlegene.parent.process.runners.options;

import org.apache.beam.sdk.options.Description;

/**
 * @Classname BaseFunPipelineOptions
 * @Description TODO
 * @Date 2020/7/3 15:51
 * @Created by limeng
 */
public interface BaseFunPipelineOptions {
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
