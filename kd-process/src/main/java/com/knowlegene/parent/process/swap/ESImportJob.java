package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.model.SwapOptions;
import com.knowlegene.parent.process.transform.TypeConversion;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * @Author: limeng
 * @Date: 2019/9/9 18:30
 */
public class ESImportJob extends ImportJobBase {
    public ESImportJob() {

    }

    public ESImportJob(SwapOptions opts) {
        super(opts);
    }

    /**
     * get matchall
     * @return
     */
    private PCollection<String> queryAll(){
        String[] esAddrs = options.getEsAddrs();
        String esIndex = options.getEsIndex();
        String esType = options.getEsType();

        ElasticsearchIO.Read read = geteSSwap().getRead(esAddrs, esIndex, esType);
        if(read == null){
            getLogger().error("es read is null");
            return null;
        }else{
            String esQuery = options.getEsQuery();
            if(BaseUtil.isNotBlank(esQuery)){
                read.withQuery(esQuery);
            }
            getLogger().info("queryall start=>index:{},type:{}",esIndex,esType);
            return super.getPipeline().apply(read);
        }
    }

    /**
     *
     * 转换 根据导入库的类型
     * @return
     */
    private PCollection<Row> transformPCollection(){
        PCollection<Row> result = null;
        Schema type= null;
        if(isMySQL()){
            type = getMysqlSchemas();
        }else if(isHiveImport()){
            type = getHiveSchemas(true);
        }
        if(type == null){
            getLogger().error("schema is null");
            return result;
        }
        PCollection<String> strings = queryAll();
        if(strings != null){
            return strings.apply(ParDo.of(new TypeConversion.JsonAndRow()));
        }
        return result;
    }


    @Override
    public PCollection<Row> query() {
        return transformPCollection();
    }
}
