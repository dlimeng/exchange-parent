package com.knowlegene.parent.process.io.file;

import com.knowlegene.parent.process.transform.PrintTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.beam.sdk.transforms.Watch.Growth.afterTimeSinceNewOutput;

/**
 * @Author: limeng
 * @Date: 2019/9/4 19:42
 */
@RunWith(JUnit4.class)
public class FileChangTest {
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();
    @BeforeClass
    public static void beforeClass(){

    }
    @AfterClass
    public static void  afterClass(){

    }
    @Test
    public void testRead(){
        PCollection<String> apply = pipeline.apply(TextIO.read().from("D:\\工具\\workspace_new\\kd-parent\\kd-process\\src\\main\\resources\\template\\test-00000-of-00005.cvs")
        .withHintMatchesManyFiles());
       apply.apply(ParDo.of(new PrintTransform.PrintByString()));
        pipeline.run();
    }

}
