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
    //合并成数组的列名称
    private Map<String,String[]> nestings;

    private String nestingKeysName;





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
