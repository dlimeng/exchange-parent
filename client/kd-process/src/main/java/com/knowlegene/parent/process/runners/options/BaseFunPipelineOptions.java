package com.knowlegene.parent.process.runners.options;

import org.apache.beam.sdk.options.Description;

import java.io.Serializable;

/**
 * 自定义函数
 * @Classname BaseFunPipelineOptions
 * @Description TODO
 * @Date 2020/7/3 15:51
 * @Created by limeng
 */
public interface BaseFunPipelineOptions  extends Serializable {
    /**
     * 嵌套json
     * @return
     */
    @Description("db.嵌套keys")
    String[] getNestingKeys();
    void setNestingKeys(String[] keys);
    @Description("db.嵌套列")
    String[] getNestingValues();
    void setNestingValues(String[] nestings);
    @Description("db.嵌套列名称")
    String getNestingKeysName();
    void setNestingKeysName(String nestingKeysName);
}
