package com.knowlegene.parent.process.transform;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/9/12 15:53
 */
public class ESTransform {

    /**
     * es提前JsonNode
     */
    public static class ExtractValueFn implements ElasticsearchIO.Write.FieldValueExtractFn{
        private final String fieldName;

        public ExtractValueFn(String fieldName) {
            this.fieldName = fieldName;
        }
        @Override
        public String apply(JsonNode input) {
            return input.path(fieldName).asText();
        }
    }

    public static class NestingFieldTransform extends PTransform<PCollection<Row>, PCollection<Row>>{
        Logger logger =  LoggerFactory.getLogger(this.getClass());
        private final List<String> keys;
        private final Schema type;
        //嵌套字段名称
        private final KV<String,List<String>> nestings;

         public NestingFieldTransform(List<String> keys, Schema type, KV<String, List<String>> nestings) {
             this.keys = keys;
             this.type = type;
             this.nestings = nestings;
         }

         public NestingFieldTransform(@Nullable String name, List<String> keys, Schema type, KV<String, List<String>> nestings) {
             super(name);
             this.keys = keys;
             this.type = type;
             this.nestings = nestings;
         }

        @Override
        public PCollection<Row> expand(PCollection<Row> input) {
             PCollection<Row> merages = null;
             if(input == null){
                 return merages;
             }

            merages = input.apply(ParDo.of(new FilterTransform.FilterByKeys(keys)))
                     .apply(Combine.perKey(new CombineTransform.UniqueSets()))
                     .apply(ParDo.of(new FilterTransform.FilterKeysAndJson(type, nestings)))
                     .setCoder(SchemaCoder.of(type));

            return merages;
        }
    }

}
