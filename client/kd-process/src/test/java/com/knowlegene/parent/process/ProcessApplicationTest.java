package com.knowlegene.parent.process;

import com.knowlegene.parent.process.runners.options.IndexerPipelineOptions;
import com.knowlegene.parent.config.util.PipelineSingletonUtil;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;


/**
 * @Author: limeng
 * @Date: 2019/8/2 14:19
 */
public class ProcessApplicationTest {
    /**
     * 设置参数
     * @param options
     */
   public void setPipelineOptions(IndexerPipelineOptions options){


       PipelineOptionsFactory.register(IndexerPipelineOptions.class);
       Pipeline pipeline = Pipeline.create(options);
       PipelineSingletonUtil.getInstance(pipeline);


       pipeline.run().waitUntilFinish();
   }

    /**
     * 无格式的单条sql语句
     */
    @Test
    public void importByProcess(){
        IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs("").withValidation().as(IndexerPipelineOptions.class);
        options.setRunner(DirectRunner.class);
        options.setPattern("import");
        options.setSourceName("hive");
        options.setJdbcSql("insert into a_mart.test values(\"444\",\"text\")");
        //options.setJdbcSql("set hive.exec.dynamic.partition=true");
        this.setPipelineOptions(options);
    }

    /**
     * 处理简单格式文件，无序
     */
    @Test
    public void importByProcessFile(){
        IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs("").withValidation().as(IndexerPipelineOptions.class);
        options.setRunner(DirectRunner.class);

        options.setSqlFile(true);
        options.setPattern("import");
        options.setSourceName("hive");
        //默认是无序的
        options.setOrderLink(false);
        options.setFilePath("D:\\工具\\workspace_new\\kd-parent\\kd-process\\src\\main\\resources\\template\\sqlfile");

        this.setPipelineOptions(options);
    }

    /**
     * 处理简单格式文件，有序
     */
    @Test
    public void importByProcessFileAndOrder(){
        IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs("").withValidation().as(IndexerPipelineOptions.class);
        options.setRunner(DirectRunner.class);

        options.setSqlFile(true);
        options.setPattern("import");
        options.setSourceName("hive");
        //默认是无序的
        options.setOrderLink(true);
        options.setFilePath("D:\\工具\\workspace_new\\kd-parent\\kd-process\\src\\main\\resources\\template\\sqlfile");


        this.setPipelineOptions(options);
    }



    /**
     * 处理有格式的sql
     *
     */
    @Test
    public void importByProcessTree(){
        IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs("").withValidation().as(IndexerPipelineOptions.class);
        options.setRunner(DirectRunner.class);
        options.setPattern("import");
        options.setSourceName("hive");
        //格式标志位
        options.setTreeStatus(true);
        String json1 ="{\"optionType\":\"insert\",\"select\":\"insert into a_mart.test2019 values('12','222')\"}";
        String json2="{\"optionType\":\"insert\",\"insert\":\"insert overwrite table  a_mart.test2019\",\"select\":\"select id,name\",\"from\":\"from a_mart.test2019\"}";
        options.setJdbcSql(json2);

        this.setPipelineOptions(options);
    }



    /**
     * 处理有格式的sql文件
     * 无序
     */
    @Test
    public void importByProcessTreeFile(){
        IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs("").withValidation().as(IndexerPipelineOptions.class);
        options.setRunner(DirectRunner.class);
        options.setPattern("import");
        options.setSourceName("hive");
        //格式标志位
        options.setTreeStatus(true);
        //默认是无序的
        options.setOrderLink(false);

        options.setFilePath("");
        this.setPipelineOptions(options);
    }


    /**
     * 处理有格式的sql文件
     * 有序
     */
    @Test
    public void importByProcessTreeFileAndOrder(){
        IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs("").withValidation().as(IndexerPipelineOptions.class);
        options.setRunner(DirectRunner.class);
        options.setPattern("import");
        options.setSourceName("hive");
        options.setFilePath("");
        //默认是无序的
        options.setOrderLink(true);
        this.setPipelineOptions(options);
    }
}
