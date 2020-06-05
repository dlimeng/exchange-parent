package com.knowlegene.parent.process.transform;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.Row;

import java.util.HashSet;
import java.util.Set;

/**
 * 合并
 * @Author: limeng
 * @Date: 2019/9/17 16:17
 */
public class CombineTransform {
    public static class UniqueSets extends Combine.CombineFn<Row, Set<Row>, Set<Row>>{
        @Override
        public Set<Row> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<Row> addInput(Set<Row> accumulator, Row input) {
            if(input != null){
                accumulator.add(input);
            }
            return accumulator;
        }

        @Override
        public Set<Row> mergeAccumulators(Iterable<Set<Row>> accumulators) {
            Set<Row> all = new HashSet<>();
            for (Set<Row> part : accumulators) {
                all.addAll(part);
            }
            return all;
        }

        @Override
        public Set<Row> extractOutput(Set<Row> accumulator) {
            return accumulator;
        }
    }
}
