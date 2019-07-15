package com.knowlegene.parent.process.kafka;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @Author: limeng
 * @Date: 2019/7/15 11:56
 */
public class KafkaConsumerTest {
    static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();


        Pipeline p = Pipeline.create(options);

        p.apply(KafkaIO.<Long, String>read()
                .withBootstrapServers("kafka:29092")
                .withTopic("words")
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)

                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))

                .withMaxNumRecords(5)

                .withoutMetadata() // PCollection<KV<Long, String>>
        )
                .apply(Values.<String>create())
                .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (String word : c.element().split(TOKENIZER_PATTERN)) {
                            if (!word.isEmpty()) {
                                c.output(word);
                            }
                        }
                    }
                }))
                .apply(Count.<String>perElement())
                .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input) {
                        return input.getKey() + ": " + input.getValue();
                    }
                }))
                .apply(TextIO.write().to("wordcounts"));

        p.run().waitUntilFinish();
    }
}
