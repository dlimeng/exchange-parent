package com.knowlegene.parent.config.util;

import com.knowlegene.parent.config.model.CustomizeHadoopOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * 获取文件
 * @Author: limeng
 * @Date: 2019/7/23 20:43
 */
public class HdfsPipelineUtil implements Serializable {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String START_HADOOP_PATH ="hdfs";
    /**
     * 判断
     * @param path 路径
     * @return
     */
    public static Boolean isHadoopPath(String path){
        if(path.trim().startsWith(START_HADOOP_PATH)){
            return true;
        }
        return false;
    }


    /**
     * 文件集合
     */
    public static PCollection<String> startDocumentImport(String path){
        PCollection<String> result=null;
        Pipeline pipeline = PipelineSingletonUtil.instance;
        PipelineOptions pipelineOptions = pipeline.getOptions();
        if(BaseUtil.isNotBlank(path)){
            if(isHadoopPath(path)){
                result = hdfsDocumentImport(path);
            }else{
                result = fileDocumentImport(path);
            }
        }
        return result;
    }

    /**
     * 获取hadoop文件
     * @return
     */
    public static PCollection<String> hdfsDocumentImport(String filePath){
        Pipeline pipeline = initPipeline();
        return pipeline.apply("hdfsDocumentImport", TextIO.read().from(filePath));
    }

    /**
     * 或去本地文件
     * @return
     */
    public static PCollection<String>  fileDocumentImport(String filePath){
        Pipeline pipeline = PipelineSingletonUtil.instance;
        return pipeline.apply("fileDocumentImport", TextIO.read().from(filePath));

    }

    /**
     * 初始化pipline
     * @return
     */
    public static  Pipeline initPipeline(){
        CustomizeHadoopOptions options = PipelineOptionsFactory.create().as(CustomizeHadoopOptions.class);
        Configuration conf = new Configuration();
        Properties properties = PropertiesUtil.getProperties();
        String defaultName = properties.getProperty("kd.hdfs.defaultname");
        String hdfsImpl = properties.getProperty("kd.hdfs.impl");
        conf.set("fs.default.name", defaultName);
        conf.set("fs.hdfs.impl", hdfsImpl);
        List<Configuration> list = new ArrayList<Configuration>();
        list.add(conf);
        options.setHdfsConfiguration(list);
        return Pipeline.create(options);
    }


}
