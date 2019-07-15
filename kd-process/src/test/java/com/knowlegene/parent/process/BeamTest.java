package com.knowlegene.parent.process;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/7/15 11:18
 */
public class BeamTest {

    static final List<String> INPUTS =
            Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");

    static class EvenNumberFn extends DoFn<Integer, Integer> {
        @ProcessElement
        public void processElement(@Element Integer in, OutputReceiver<Integer> out) {
            if (in % 2 == 0) {
                out.output(in);
            }
        }
    }

    static class ParseIntFn extends DoFn<String, Integer> {
        @ProcessElement
        public void processElement(@Element String in, OutputReceiver<Integer> out) {
            out.output(Integer.parseInt(in));
        }
    }

    public void testFn() {
        Pipeline p = TestPipeline.create();
        PCollection<String> input = p.apply(Create.of(INPUTS)).setCoder(StringUtf8Coder.of());
        PCollection<Integer> output1 = input.apply(ParDo.of(new ParseIntFn())).apply(ParDo.of(new EvenNumberFn()));
        PAssert.that(output1).containsInAnyOrder(2, 4, 6, 8, 10);
        PCollection<Integer> sum = output1.apply(Combine.globally(Sum.ofIntegers()));
        PAssert.that(sum).equals(30);
        p.run();
    }
}
