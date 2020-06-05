package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.process.model.SwapOptions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * @Author: limeng
 * @Date: 2019/9/23 17:46
 */
public class Neo4jImportJob extends ImportJobBase{
    public Neo4jImportJob() {
    }

    public Neo4jImportJob(SwapOptions opts) {
        super(opts);
    }

    @Override
    public PCollection<Row> query() {
        return super.query();
    }
}
