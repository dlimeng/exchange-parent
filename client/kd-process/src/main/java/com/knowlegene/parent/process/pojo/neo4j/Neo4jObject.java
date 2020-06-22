package com.knowlegene.parent.process.pojo.neo4j;

import com.knowlegene.parent.config.util.BaseUtil;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/9/23 17:12
 */
@Data
public class Neo4jObject  implements Serializable {
    private Map<String,Object> parMap;
    private Object[] Objects;

    public Object[] getObjectValue(){
        Object[] result=null;
        if(!BaseUtil.isBlankMap(parMap)){
            List<Object> list=new ArrayList<>();
            parMap.forEach((k,v)->{
                list.add(k);
                list.add(v);
            });
            result = list.toArray();
        }else if(Objects != null || Objects.length>0){
            result = Objects;
        }
        return result;
    }
}
