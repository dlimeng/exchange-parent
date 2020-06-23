package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.common.constantenum.Neo4jEnum;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jObject;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jOptions;
import com.knowlegene.parent.process.transform.TypeConversion;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.List;


/**
 * @Author: limeng
 * @Date: 2019/9/23 17:46
 */
public class Neo4jExportJob extends ExportJobBase  {

    public Neo4jExportJob() {
    }

    public Neo4jExportJob(SwapOptions options) {
        super(options);
    }

//    @Override
//    public PCollection<Row> query() {
//        return super.query();
//    }
}
