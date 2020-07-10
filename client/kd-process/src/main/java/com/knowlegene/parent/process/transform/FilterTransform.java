package com.knowlegene.parent.process.transform;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.util.CommonUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 过滤自定义
 * @Author: limeng
 * @Date: 2019/9/2 15:56
 */
public class FilterTransform {
    public static  class FilterOldData extends DoFn<HCatRecord, HCatRecord>{
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        private PCollectionView<Iterable<HCatRecord>> it=null;
        private HCatSchema hCatSchema=null;
        private String mark;

        @Setup
        public void setup(){
            logger.info("filter old data start");
        }

        public FilterOldData(PCollectionView<Iterable<HCatRecord>> it, HCatSchema hCatSchema, String mark) {
            this.it = it;
            this.hCatSchema = hCatSchema;
            this.mark = mark;
        }

        @ProcessElement
        public void processElement(@Element HCatRecord hr1, OutputReceiver<HCatRecord> out, ProcessContext c) {
            Iterable<HCatRecord> rows = c.sideInput(it);
            Iterator<HCatRecord> iterator = rows.iterator();
            boolean status =true;
            while (iterator.hasNext()){
                HCatRecord next = iterator.next();
                String version2 = null;
                try {
                    version2 = next.get(mark,hCatSchema).toString();
                    String version1 = hr1.get(mark,hCatSchema).toString();
                    if(version1.equals(version2)){
                        //新版修改数据输出
                        out.output(next);
                        status = false;
                    }
                } catch (HCatException e) {
                    logger.error("filter is error");
                }
            }
            if(status){
                out.output(hr1);
            }
        }
    }

    /**
     * 过滤，根据key
     */
    public static  class FilterByKeysMap extends DoFn<Map<String, ObjectCoder>, KV<String, Map<String, ObjectCoder>>>{
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        private final List<String> keys;
        private Map<String, ObjectCoder> element = null;
        public FilterByKeysMap(List<String> keys) {
            this.keys = keys;
        }
        @Setup
        public void setup(){
            logger.info("filter keys start");
        }
        @ProcessElement
        public void processElement(ProcessContext pc){
            element = pc.element();
            String encrypt = getEncrypt(keys, element);
            if(BaseUtil.isNotBlank(encrypt) && !BaseUtil.isBlankMap(element)){
                pc.output(KV.of(encrypt, element));
            }
        }

        /**
         * 唯一值
         * @param keys 多个key
         * @return
         */
        private String getEncrypt(List<String> keys,Map<String, ObjectCoder> row){
            String result="";
            if(!BaseUtil.isBlankSet(keys) && !BaseUtil.isBlankMap(row)){
                Iterator<String> iterator = keys.iterator();
                StringBuffer sb = new StringBuffer();
                while (iterator.hasNext()){
                    String next = iterator.next();
                    Object value = row.get(next).getValue();
                    if(value !=  null){
                        sb.append(value.toString());
                    }
                }
                result = CommonUtil.encryptStr(sb.toString());
            }
            return result;
        }
    }
    /**
     * 过滤，根据key
     */
    public static  class FilterByKeys extends DoFn<Row, KV<String, Row>>{
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        private final List<String> keys;
        public FilterByKeys(List<String> keys) {
            this.keys = keys;
        }
        @Setup
        public void setup(){
            logger.info("filter keys start");
        }

        @ProcessElement
        public void processElement(ProcessContext pc){
            Row element = pc.element();
            String encrypt = getEncrypt(keys, element);
            if(BaseUtil.isNotBlank(encrypt) && element!=null){
                KV<String, Row> result = KV.of(encrypt, element);
                pc.output(result);
            }
        }


        /**
         * 唯一值
         * @param keys 多个key
         * @return
         */
        private String getEncrypt(List<String> keys,Row row){
            String result="";
            if(!BaseUtil.isBlankSet(keys) && row!=null){
                Iterator<String> iterator = keys.iterator();
                StringBuffer sb = new StringBuffer();
                while (iterator.hasNext()){
                    String next = iterator.next();
                    Object value = row.getValue(next);
                    if(value !=  null){
                        sb.append(value.toString());
                    }
                }
                result = CommonUtil.encryptStr(sb.toString());
            }
            return result;
        }
    }

    /**
     * 合并出，转换成嵌套形式
     */
    public static  class FilterKeysAndMapJson extends DoFn<KV<String, Set<Map<String, ObjectCoder>>>, Map<String, ObjectCoder>>{
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        //嵌套字段名称
        private final KV<String,List<String>> mergeds;
        private Map<String, ObjectCoder> result;
        private Set<Map<String, ObjectCoder>> value;
        private final String keysName;

        @Setup
        public void setup(){
            logger.info("filter to json start");
        }

        public FilterKeysAndMapJson(KV<String,List<String>> mergeds,String keysName) {
            this.mergeds = mergeds;
            this.keysName = keysName;
        }

        @ProcessElement
        public void processElement(ProcessContext pc) {
            value = pc.element().getValue();
            if (!BaseUtil.isBlankSet(value)) {
                String key = mergeds.getKey();
                List<String> mergedsValue = mergeds.getValue();
                result = new LinkedHashMap<>();
                Iterator<Map<String, ObjectCoder>> iterator = value.iterator();
                boolean first =true;
                while (iterator.hasNext()) {
                    Map<String, ObjectCoder> next = iterator.next();
                    if (!BaseUtil.isBlankMap(next)) {
                        for(Map.Entry<String, ObjectCoder> map:next.entrySet()){
                            String name = map.getKey();
                            ObjectCoder objectCoder = map.getValue();
                            Object fieldValue = objectCoder.getValue();
                            if(first && BaseUtil.isNotBlank(key)){
                                String json = getJson(value, mergeds.getValue());
                                result.put(keysName,new ObjectCoder(json,Schema.FieldType.STRING));
                            }
                            first = false;
                            if(!mergedsValue.contains(name)){
                                if (fieldValue == null) {
                                    fieldValue = "";
                                }
                                result.put(name,new ObjectCoder(fieldValue,objectCoder.getFieldType()));
                            }
                        }

                    }
                }

                if (!BaseUtil.isBlankMap(result)) {
                    pc.output(result);
                }
            }
        }
            /**
             * 集合转换嵌套json
             * @param value
             * @param mergeds
             * @return
             */
            private String getJson(Set<Map<String, ObjectCoder>> value,List<String> mergeds){
                String result="";
                if(!BaseUtil.isBlankSet(mergeds) && !BaseUtil.isBlankSet(value)){
                    Iterator<Map<String, ObjectCoder>> iterator = value.iterator();
                    StringBuffer mergedJson = new StringBuffer();
                    StringBuffer columnJson2=null;
                    String columnJson1="{%s},";
                    mergedJson.append("[");
                    while (iterator.hasNext()){
                        Map<String, ObjectCoder> next = iterator.next();
                        if(!BaseUtil.isBlankMap(next)){
                            columnJson2 = new StringBuffer();
                            boolean first=false;
                            for(String column:mergeds){
                                Object columnValue = next.get(column).getValue();
                                if(first){
                                    columnJson2.append(",");
                                }
                                first = true;
                                columnJson2.append("\"").append(column).append("\":\"").append(columnValue).append("\"");
                            }
                            mergedJson.append(String.format(columnJson1, columnJson2.toString()));
                        }
                    }
                    int length = mergedJson.length();
                    if(length > 1){
                        mergedJson.deleteCharAt(length-1);
                        mergedJson.append("]");
                        return mergedJson.toString();
                    }else{
                        return result;
                    }
                }
                return result;
            }

    }

    /**
     * 合并出，转换成嵌套形式
     */
    public static  class FilterKeysAndJson extends DoFn<KV<String, Set<Row>>, Row>{
        private Logger logger = LoggerFactory.getLogger(this.getClass());
        private final Schema type;
        //嵌套字段名称
        private final KV<String,List<String>> mergeds;
        private List<Object> result;

        @Setup
        public void setup(){
            logger.info("filter to json start");
        }

        public FilterKeysAndJson(Schema type,KV<String,List<String>> mergeds) {
            this.type = type;
            this.mergeds = mergeds;
        }

        @ProcessElement
        public void processElement(ProcessContext pc){
            Set<Row> value = pc.element().getValue();
            if (!BaseUtil.isBlankSet(value)) {
                String key = mergeds.getKey();
                result=new ArrayList<>();
                Iterator<Row> iterator = value.iterator();
                while (iterator.hasNext()){
                    Row next = iterator.next();
                    if(next!=null){
                        List<String> fieldNames = type.getFieldNames();
                        for(String fieldName:fieldNames){
                            if(fieldName.equalsIgnoreCase(key)){
                                String json = getJson(value, mergeds.getValue());
                                result.add(json);
                            }else{
                                Object fieldValue = next.getValue(fieldName);
                                if(fieldValue == null){
                                    fieldValue = "";
                                }
                                result.add(fieldValue);
                            }
                        }
                        break;
                    }
                }

                if(!BaseUtil.isBlankSet(result)){
                    Row build = Row.withSchema(type).addValues(result).build();
                    pc.output(build);
                }
            }
        }

        /**
         * 集合转换嵌套json
         * @param value
         * @param mergeds
         * @return
         */
        private String getJson(Set<Row> value,List<String> mergeds){
            String result="";
            if(!BaseUtil.isBlankSet(mergeds) && !BaseUtil.isBlankSet(value)){
                Iterator<Row> iterator = value.iterator();
                StringBuffer mergedJson = new StringBuffer();
                StringBuffer columnJson2=null;
                String columnJson1="{%s},";
                mergedJson.append("[");
                while (iterator.hasNext()){
                    Row next = iterator.next();
                    if(next != null){
                        columnJson2 = new StringBuffer();
                        boolean first=false;
                        for(String column:mergeds){
                            Object columnValue = next.getValue(column);
                            if(first){
                                columnJson2.append(",");
                            }
                            first = true;
                            columnJson2.append("\"").append(column).append("\":\"").append(columnValue).append("\"");
                        }
                        mergedJson.append(String.format(columnJson1, columnJson2.toString()));
                    }
                }
                int length = mergedJson.length();
                if(length > 1){
                    mergedJson.deleteCharAt(length-1);
                    mergedJson.append("]");
                    return mergedJson.toString();
                }else{
                    return result;
                }
            }
            return result;
        }


    }

}
