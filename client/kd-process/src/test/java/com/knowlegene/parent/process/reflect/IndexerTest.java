package com.knowlegene.parent.process.reflect;

import com.knowlegene.parent.process.runners.options.IndexerPipelineOptions;
import com.knowlegene.parent.config.util.BaseUtil;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * @Author: limeng
 * @Date: 2019/8/29 17:03
 */
public class IndexerTest {
    @Test
    public void copyObject(){
        IndexerPipelineOptions options = PipelineOptionsFactory.fromArgs("").withValidation().as(IndexerPipelineOptions.class);
        options.setPattern("p1");
        options.setSourceName("s1");
        options.setRunner(DirectRunner.class);
        Indexer indexer = new Indexer();
        BaseUtil.copyNonNullProperties(indexer,options);
        Assert.assertNotNull(indexer);
    }
}
