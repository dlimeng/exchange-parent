package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.event.ESExportType;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.pojo.es.ESOptions;
import com.knowlegene.parent.process.swap.event.ESExportTaskEvent;
import com.knowlegene.parent.process.transform.TypeConversion;
import com.knowlegene.parent.scheduler.event.EventHandler;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * @Author: limeng
 * @Date: 2019/9/9 18:30
 */
public class ESExportJob extends ExportJobBase {
    private static volatile ESOptions esOptions= null;

    public ESExportJob() {

    }

    public ESExportJob(SwapOptions options) {
        super(options);
    }

    public static ESOptions getDbOptions(){
        if(esOptions == null){
            String name = DBOperationEnum.ES_EXPORT.getName();
            Object options = getOptions(name);
            if(options != null){
                esOptions = (ESOptions)options;
            }
        }
        return esOptions;
    }




    /**
     * get matchall
     * @return
     */
    private static PCollection<String> queryAll(){
        String[] esAddrs = getDbOptions().getEsAddrs();
        String esIndex = getDbOptions().getEsIndex();
        String esType = getDbOptions().getEsType();

        ElasticsearchIO.Read read = getESSwap().getRead(esAddrs, esIndex, esType);
        if(read == null){
            getLogger().error("es read is null");
            return null;
        }else{
            String esQuery = getDbOptions().getEsQuery();
            if(BaseUtil.isNotBlank(esQuery)){
                read.withQuery(esQuery);
            }
            getLogger().info("queryall start=>index:{},type:{}",esIndex,esType);
            return getPipeline().apply(read);
        }

    }

    /**
     *
     * 转换 根据导入库的类型
     * @return
     */
    private static PCollection<Row> transformPCollection(){
        PCollection<Row> result = null;
        PCollection<String> strings = queryAll();
        if(strings != null){
            return strings.apply(ParDo.of(new TypeConversion.JsonAndRow()));
        }
        return result;

    }



    public static PCollection<Row> query() {
        return  transformPCollection();
    }


    public static class ESExportDispatcher implements EventHandler<ESExportTaskEvent> {
        @Override
        public void handle(ESExportTaskEvent event) {
            if (event.getType() == ESExportType.T_EXPORT) {
                getLogger().info("ESExportDispatcher is start");

                PCollection<Row> rows = query();
                CacheManager.setCache(DBOperationEnum.PCOLLECTION_QUERYS.getName(), rows);

            }
        }
    }
}
