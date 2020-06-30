package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.process.pojo.SwapOptions;


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
