package com.knowlegene.parent.config.model;

import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Description;

/**
 * 自定义hadoop file
 * @Author: limeng
 * @Date: 2019/7/23 22:01
 */
public interface CustomizeHadoopOptions extends HadoopFileSystemOptions {
    @Description("input file")
    String getInputFile();
    void setInputFile(String in);

    @Description("output")
    String getOutput();
    void setOutput(String out);
}
