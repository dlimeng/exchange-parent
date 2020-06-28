package com.knowlegene.parent.process.runners.common;

import com.knowlegene.parent.process.runners.options.IndexerPipelineOptions;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.PipelineSingletonUtil;
import com.knowlegene.parent.process.common.constantenum.OptionsEnum;
import lombok.Data;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 执行类
 * @Author: limeng
 * @Date: 2019/8/12 10:28
 */
@Data
public class BaseRunners implements Serializable, OptionsEnum {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private Class<? extends PipelineOptions> clazz = null;
    private Pipeline pipeline;
    private PipelineOptions pipelineOptions;

    private static final String MARK_SPLIT = "\\|%a%b%c%\\|";
    private static final String UNDERLINE_T = "_";


    public BaseRunners() {
    }

    public BaseRunners(Class<? extends PipelineOptions> clazz) {
        this.clazz = clazz;
    }



    public void start(Map<String, Object> map){
        String[] args = mapToStr(map);
        pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(clazz);
        pipeline = Pipeline.create(pipelineOptions);
        PipelineSingletonUtil.getInstance(pipeline);
    }

    public void startByArgs(String[] args){
        pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(clazz);
        pipeline = Pipeline.create(pipelineOptions);
        PipelineSingletonUtil.getInstance(pipeline);
    }

    public void start(){
        if(clazz == null){
            pipelineOptions = PipelineOptionsFactory.create();
        }else {
            pipelineOptions = PipelineOptionsFactory.fromArgs("").withValidation().as(clazz);
        }

        pipeline = Pipeline.create(pipelineOptions);
        PipelineSingletonUtil.getInstance(pipeline);
    }

    public void run(){

        pipeline.run().waitUntilFinish();
    }

    /**
     * map转str
     * @param map
     * @return
     */
    private String[] mapToStr(Map<String, Object> map){
        String[] result=null;
        if(!BaseUtil.isBlankMap(map)){
            result = new String[map.size()];
            int index=0;
            for(Map.Entry<String,Object> entry : map.entrySet()){
                result[index] = "--"+entry.getKey()+"="+entry.getValue().toString();
                index++;
            }
        }else{
            logger.info("fromArgs is null");
        }
        return result;
    }



    public static void main(String[] args) {

        BaseRunners runners = new BaseRunners(IndexerPipelineOptions.class);

        HashMap<String, Object> objectObjectHashMap = new HashMap<>();
        objectObjectHashMap.put("pattern","cccc");
        runners.start(objectObjectHashMap);
        Pipeline instance = PipelineSingletonUtil.instance;
        IndexerPipelineOptions options = (IndexerPipelineOptions) instance.getOptions();
        System.out.println(options);
    }
}
