package com.knowlegene.parent.process.pojo;

import com.knowlegene.parent.config.util.BaseUtil;
import lombok.Data;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 嵌套对象
 * @Author: limeng
 * @Date: 2019/9/16 20:01
 */
@Data
public class NestingFields {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    //唯一key名称
    private String[] keys;
    //普通列名称
    private String[] columns;
    //合并成数组的列名称
    private Map<String,String[]> nestings;
    //表名称
    private String tableName;
    //去重完列名
    private String[]  distinctColumns;
    //结果表类型
    private Schema resultSchema;
    //查询表类型
    private Schema querySchema;


    public boolean isEmpty(){
        boolean result=false;
        if(BaseUtil.isBlank(tableName)){
            logger.error("table is null");
            result = true;
        }
        if(keys == null || keys.length<=0){
            logger.error("keys is null");
            result = true;
        }
        if(BaseUtil.isBlankMap(nestings)){
            logger.error("nestings is null");
            result = true;
        }
        return result;
    }

    /**
     * 拼装查询sql
     * @return
     */
    public String nestingSelectSQL(){
        StringBuffer sb = new StringBuffer();
        KV<String, String[]> stringKV = mapToKV();
        if(stringKV == null){
            return "";
        }
        //唯一key
        String[] s2 = distinctColumns(distinctColumns, stringKV.getValue());
        if(s2 == null || s2.length<=0){
            return "";
        }

        boolean first=false;
        List<Schema.Field> fields=new ArrayList<>();
        for(String s:s2){
            if(first){
                sb.append(",");
            }
            first = true;
            sb.append(s);
            Schema.Field of = Schema.Field.of(s, Schema.FieldType.STRING);
            fields.add(of);
        }
        querySchema = Schema.builder().addFields(fields).build();
        return "select "+sb.toString();
    }

    //普通列字段
    private String[] columnsCommon(String[] keys,String[] columns){
        String[] s1 = distinctColumns(keys, columns);
        return s1;
    }

    private String[]  distinctColumns(String[] oldColumns,String[] newColumns){
        String[] tmp=oldColumns;
        if(tmp != null && tmp.length > 0){
            if(newColumns != null && newColumns.length > 0){
                List<String> list=new ArrayList<>();
                list.addAll(Arrays.asList(tmp));
                list.addAll(Arrays.asList(newColumns));
                Set<String> set1 = new HashSet<>(list);
                int size = set1.size();
                tmp = new String[size];
                set1.toArray(tmp);
            }
        }else{
            tmp = newColumns;
        }
        return tmp;
    }

    /**
     * 创建表sql
     * 创建type
     * @param table 表名
     * @param nestingName 嵌套字段名称
     * @return
     */
    public String creatByNesting(String table,String nestingName){
        String result="";
        if(BaseUtil.isNotBlank(table) && BaseUtil.isNotBlank(nestingName)){
            String[] strings = columnsCommon(keys, columns);
            distinctColumns =strings;
            if(strings== null || strings.length<=0){
                return result;
            }

            String sql1="create table if not exists %s row format delimited fields terminated by '\\t' stored as textfile";
            StringBuffer sb = new StringBuffer();
            sb.append(table+"(");
            List<Schema.Field> fields=new ArrayList<>();
            for(String columns:strings){
                if(BaseUtil.isNotBlank(columns)){
                    sb.append(columns+" string,");
                    Schema.Field of = Schema.Field.of(columns, Schema.FieldType.STRING);
                    fields.add(of);
                }
            }
            sb.append(nestingName+" string)");
            Schema.Field of = Schema.Field.of(nestingName, Schema.FieldType.STRING);
            fields.add(of);
            result = String.format(sql1,sb.toString());
            resultSchema = Schema.builder().addFields(fields).build();
        }
        return result;
    }

    public void creatByNesting(String nestingName,Schema schema){
        if(BaseUtil.isNotBlank(nestingName) && schema!=null){
            String[] strings = columnsCommon(keys, columns);
            distinctColumns =strings;
            if(strings== null || strings.length<=0){
                return ;
            }

            List<Schema.Field> fields=new ArrayList<>();
            for(String columns:strings){
                if(BaseUtil.isNotBlank(columns)){
                       fields.add(schema.getField(columns));
                }
            }
            Schema.Field of = Schema.Field.of(nestingName, Schema.FieldType.STRING);
            fields.add(of);
            resultSchema = Schema.builder().addFields(fields).build();
        }
    }

    /**
     * 合并字段转换kv
     * @return
     */
    public KV<String,String[]> mapToKV(){
        Iterator<String> iterator = nestings.keySet().iterator();
        while (iterator.hasNext()){
            String next = iterator.next();
            if(BaseUtil.isNotBlank(next)){
                String[] values = nestings.get(next);
                return KV.of(next.toLowerCase(),values);
            }
        }
        return null;
    }

    /**
     * 合并字段转换kv
     * @return
     */
    public KV<String,List<String>> mapToKV2(){
        Iterator<String> iterator = nestings.keySet().iterator();
        while (iterator.hasNext()){
            String next = iterator.next();
            if(BaseUtil.isNotBlank(next)){
                String[] values = nestings.get(next);
                return KV.of(next.toLowerCase(),Arrays.asList(values));
            }

        }
        return null;
    }
}
