package com.knowlegene.parent.process.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/9/2 18:45
 */
public class PrintTransform {
    public static class  Print extends DoFn<Row,Row> {
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        @ProcessElement
        public void processElement( ProcessContext c) {
            Row element = c.element();
            List<Object> values = element.getValues();
            StringBuilder stringBuilder = new StringBuilder();
            for (Object o:values){
                stringBuilder.append(o.toString()+" ");
            }
            logger.info(stringBuilder.toString());
            c.output(element);
        }
    }

    public static class  PrintByHCatIO extends DoFn<HCatRecord,HCatRecord> {
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        @ProcessElement
        public void processElement( ProcessContext c) {
            HCatRecord element = c.element();
            List<Object> all = element.getAll();
            StringBuilder stringBuilder = new StringBuilder();
            for (Object o:all){
                stringBuilder.append(o.toString()+" ");
            }
            logger.info(stringBuilder.toString());
            c.output(element);
        }
    }

    public static class  PrintByString extends DoFn<String,String> {
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        @ProcessElement
        public void processElement( ProcessContext c) {
            String element = c.element();
            logger.info(element);
            c.output(element);
        }
    }

}
