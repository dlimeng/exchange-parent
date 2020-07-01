package com.knowlegene.parent.process;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.joda.time.Instant;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class TestHCatalogIO implements Serializable{

    @Test
    public void testRead() throws HCatException {
        PipelineOptions options   = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        Map<String, String> configProperties = new HashMap<String, String>();
        configProperties.put("hive.metastore.uris","thrift://m5.server:9083");
        PCollection<HCatRecord> collection = pipeline
                .apply(HCatalogIO.read()
                        .withConfigProperties(configProperties)
                        .withDatabase("default")
                        .withTable("test"));



//        Schema idSchemas = Schema.builder().addStringField("id").build();
//        Row build = Row.withSchema(idSchemas).addValue(collection).build();
//        pipeline.apply(Create.of(build)).setCoder(SchemaCoder.of(idSchemas)).apply(ParDo.of(new DoFn<Row, String>() {
//            @ProcessElement
//            public void processElement(ProcessContext c) throws HCatException {
//                Row element = c.element();
//                String id = element.getString("id");
//                System.out.println(id);
//            }
//        }));
        collection.apply(ParDo.of(new DoFn<HCatRecord, HCatRecord>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
                HCatRecord element = c.element();
                HCatFieldSchema.Type type = HCatFieldSchema.Type.STRING;
                HCatFieldSchema idSchema = new HCatFieldSchema("id", type, "");
                HCatSchema hCatSchema = new HCatSchema(Arrays.asList(idSchema));
                String id = element.getString("id", hCatSchema);
                System.out.println(id);

                if(id.equals("123"))
                    c.output(element);
            }
        })).apply(HCatalogIO.write()
                    .withConfigProperties(configProperties)
                    .withDatabase("default")
                    .withTable("test2")
                    .withBatchSize(1000L));



        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testRowAndWrite(){
        PipelineOptions options   = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        Schema idSchemas = Schema.builder().addStringField("id").build();
        Row build = Row.withSchema(idSchemas).addValue("1").build();
        pipeline.apply(Create.of(build)).setCoder(SchemaCoder.of(idSchemas)).apply(ParDo.of(new DoFn<Row, HCatRecord>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
                Row element = c.element();
                Schema schema = element.getSchema();
                int fieldCount = schema.getFieldCount();
                for (int i = 0; i < fieldCount; i++) {
                    Schema.Field field = schema.getField(i);
                    Schema.TypeName typeName = field.getType().getTypeName();
                }
            }
        }));

        pipeline.run().waitUntilFinish();
    }

    public void testTime(){

    }
}
