package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.model.SwapOptions;
import com.knowlegene.parent.process.transform.PrintTransform;
import com.knowlegene.parent.process.transform.TypeConversion;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * @Author: limeng
 * @Date: 2019/9/9 18:30
 */
public class ESExportJob extends ExportJobBase {
    public ESExportJob() {

    }

    public ESExportJob(SwapOptions options) {
        super(options);
    }

    /**
     * 更新
     * @param rows
     */
    private void update(PCollection<Row> rows){
        String[] esAddrs = options.getEsAddrs();
        String esIndex = options.getEsIndex();
        String esType = options.getEsType();
        String esIdFn = options.getEsIdFn();
        ElasticsearchIO.Write write = null;
        if(BaseUtil.isNotBlank(esIdFn)){
            write =geteSSwap().getWrite(esAddrs, esIndex, esType,esIdFn);
        }else{
            write = geteSSwap().getWrite(esAddrs, esIndex, esType);
        }

        if(write == null){
            getLogger().error("es write is null");
            return;
        }
        getLogger().info("update start=>index:{},type:{}",esIndex,esType);

        Schema schema = rows.getSchema();
        rows.apply(ParDo.of(new TypeConversion.RowAndJson(schema))).setCoder(StringUtf8Coder.of())
                .apply(write);
    }


    @Override
    public void save(PCollection<Row> rows) {
        if(rows != null){
            update(rows);
        }
    }
}
