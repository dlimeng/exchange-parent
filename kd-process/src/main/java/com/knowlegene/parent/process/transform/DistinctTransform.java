package com.knowlegene.parent.process.transform;


import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * 去重自定义
 * @Author: limeng
 * @Date: 2019/9/2 20:13
 */
public class DistinctTransform {
     static class DistinctHCatRecord extends PTransform<PCollection<HCatRecord>,PCollection<HCatRecord>>{
         private final HCatSchema hCatSchema;
         private final String version;
         private Logger logger = LoggerFactory.getLogger(this.getClass());
         public DistinctHCatRecord( HCatSchema hCatSchema, String version) {
             this.hCatSchema = hCatSchema;
             this.version = version;
         }

         @Override
         public PCollection<HCatRecord> expand(PCollection<HCatRecord> input) {
             //去重
             PCollection<HCatRecord> result = input.apply(Distinct.withRepresentativeValueFn(new SerializableFunction<HCatRecord, String>() {
                 @Override
                 public String apply(HCatRecord input) {
                     try {
                         return input.get(version,hCatSchema).toString();
                     } catch (HCatException e) {
                         logger.error("distinct is error =>msg:{}",e.getMessage());
                         return null;
                     }
                 }
             }).withRepresentativeType(TypeDescriptor.of(String.class)));

             return result;
         }
     }
}
