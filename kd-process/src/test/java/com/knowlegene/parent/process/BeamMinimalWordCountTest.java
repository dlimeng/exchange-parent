package com.knowlegene.parent.process;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

/**
 * @Author: limeng
 * @Date: 2019/7/15 10:47
 */
public class BeamMinimalWordCountTest {
    @SuppressWarnings("serial")
    public static void main(String[] args) {
            PipelineOptions options = PipelineOptionsFactory.create();
            options.setRunner(DirectRunner.class); // 显式指定PipelineRunner：DirectRunner（Local模式）

            Pipeline pipeline = Pipeline.create(options);

            // 读取本地文件，构建第一个PTransform
            pipeline.apply("ReadLines", TextIO.read().from("/words"))
                    .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                        // 对文件中每一行进行处理（实际上Split）
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            for (String word : c.element().split("[\\s:\\,\\.\\-]+")) {
                                if (!word.isEmpty()) {
                                    c.output(word);
                                }
                            }
                        }

                    }))
                    .apply(Count.<String> perElement()) // 统计每一个Word的Count
                    .apply("ConcatResultKVs", MapElements.via( // 拼接最后的格式化输出（Key为Word，Value为Count）
                            new SimpleFunction<KV<String, Long>, String>() {

                                @Override
                                public String apply(KV<String, Long> input) {
                                    return input.getKey() + ": " + input.getValue();
                                }

                            }))
                    .apply(TextIO.write().to("wordcount"));
            // 输出结果

            pipeline.run().waitUntilFinish();
    }
}
