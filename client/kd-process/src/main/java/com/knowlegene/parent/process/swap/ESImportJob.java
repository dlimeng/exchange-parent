package com.knowlegene.parent.process.swap;


import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.event.ESImportType;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.pojo.NestingFields;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.pojo.es.ESOptions;
import com.knowlegene.parent.process.swap.event.ESImportTaskEvent;
import com.knowlegene.parent.process.transform.ESTransform;
import com.knowlegene.parent.process.transform.TypeConversion;
import com.knowlegene.parent.scheduler.event.EventHandler;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/9/9 18:30
 */
public class ESImportJob extends ImportJobBase {
    private static volatile ESOptions esOptions= null;

    public ESImportJob() {

    }

    public ESImportJob(SwapOptions opts) {
        super(opts);
    }


    public static ESOptions getDbOptions(){
        if(esOptions == null){
            String name = DBOperationEnum.ES_IMPORT.getName();
            Object options = getOptions(name);
            if(options != null){
                esOptions = (ESOptions)options;
            }
        }
        return esOptions;
    }
    /**
     * 更新
     * @param rows
     */
    private static void update(PCollection<Map<String, ObjectCoder>> rows){
        String[] esAddrs = getDbOptions().getEsAddrs();
        String esIndex = getDbOptions().getEsIndex();
        String esType = getDbOptions().getEsType();
        String esIdFn = getDbOptions().getEsIdFn();

        ElasticsearchIO.Write write = null;
        if(BaseUtil.isNotBlank(esIdFn)){
            write =getESSwap().getWrite(esAddrs, esIndex, esType,esIdFn);
        }else{
            write = getESSwap().getWrite(esAddrs, esIndex, esType);
        }

        if(write == null){
            getLogger().error("es write is null");
            return;
        }
        getLogger().info("update start=>index:{},type:{}",esIndex,esType);


        rows.apply(ParDo.of(new TypeConversion.mapAndJson())).setCoder(StringUtf8Coder.of())
                .apply(write);
    }

    /**
     * 嵌套
     * @param nestingFields
     * @param querys
     * @return
     */
    private static PCollection<Map<String, ObjectCoder>> nestingFieldToEs(NestingFields nestingFields, PCollection<Map<String, ObjectCoder>> querys){
        if(nestingFields == null){
            return null;
        }

        //查询
        String[] keys = nestingFields.getKeys();
        String nestingKeysName = nestingFields.getNestingKeysName();
        KV<String, List<String>> nestings = nestingFields.mapToKV2();
        if(BaseUtil.isBlank(nestingKeysName)){
            getLogger().info("nestingKeysName is null");
            return null;
        }

        if(nestings == null){
            getLogger().info("nestings is null");
            return null;
        }

        if(keys == null || keys.length == 0){
            getLogger().info("nestingFields key is null");
            return null;
        }


        return  querys.apply(new ESTransform.NestingFieldTransformMap(Arrays.asList(keys),nestings,nestingKeysName));
    }

    public static void save(PCollection<Map<String, ObjectCoder>> rows) {
        PCollection<Map<String, ObjectCoder>> reslut = rows;
        if(rows != null){
            NestingFields nestingFields = getDbOptions().getNestingFields();
            if(nestingFields != null) reslut = nestingFieldToEs(nestingFields,rows);

            update(reslut);
        }
    }


    public static class ESImportDispatcher implements EventHandler<ESImportTaskEvent> {
        @Override
        public void handle(ESImportTaskEvent event) {
            if(event.getType() == ESImportType.T_IMPORT){
                getLogger().info("ESImportDispatcher is start");

                if(CacheManager.isExist(DBOperationEnum.PCOLLECTION_QUERYS.getName())){

                    save((PCollection<Map<String, ObjectCoder>>)CacheManager.getCache(DBOperationEnum.PCOLLECTION_QUERYS.getName()));
                }

            }
        }
    }
}
