package com.knowlegene.parent.process.runners.options;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
/**
 * 参数
 * @Author: limeng
 * @Date: 2019/7/18 14:51
 */
public interface IndexerPipelineOptions extends SparkPipelineOptions {

    @Description("方式import/export")
    String getPattern();
    void setPattern(String pattern);

    @Description("源名称")
    String getSourceName();
    void setSourceName(String sourceName);

    @Description("是否批量处理sql任务")
    @Default.Boolean(false)
    Boolean isSqlFile();
    void setSqlFile(Boolean sqlFile);

    @Description("获取文件地址，本地/hdfs")
    String getFilePath();
    void setFilePath(String filePath);


    @Description("链路是否有序")
    @Default.Boolean(false)
    Boolean isOrderLink();
    void setOrderLink(Boolean value);


    @Description("jdbc sql")
    String getJdbcSql();
    void setJdbcSql(String jdbcSql);

    @Description("jdbc 树 状态")
    @Default.Boolean(false)
    Boolean isTreeStatus();
    void setTreeStatus(Boolean value);


}
