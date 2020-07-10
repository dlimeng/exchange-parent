package com.knowlegene.parent.process.pojo.neo4j;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.common.constantenum.Neo4jEnum;
import lombok.Data;

import java.util.*;

/**
 * neo4j操作
 * @Author: limeng
 * @Date: 2019/9/29 15:07
 */
@Data
public class Neo4jCode {
    private String Rex="\\s|\t|\n";
    private String dsl;
    private List<String>  keys;
    private Integer type;
    private String startId=":START_ID";
    private String endId=":END_ID";
    private String id="ID";

    /**
     * 节点属性
     * @param format
     */
    private void setAttribute(String format){
        if(BaseUtil.isNotBlank(format)){
            String[] split = format.split(Rex);
            if(split.length>0){
                keys = Arrays.asList(split);
            }
        }
    }

    /**
     * 操作
     */
    private void options(){
        if(!BaseUtil.isBlankSet(keys)){
            boolean present1 =  keys.stream().anyMatch(a -> a.contains(startId) || a.contains(endId));
            boolean present2 =  keys.stream().anyMatch(a -> a.contains(id) );
            if(present1){
                type = Neo4jEnum.RELATE.getValue();
            }else if(present2){
                type = Neo4jEnum.SAVE.getValue();
            }
        }
    }

    /**
     * 生成操作语句
     */
    private void createDSL(){
        if(!BaseUtil.isBlankSet(keys) && type!=null){
            boolean first=false;
            StringBuffer sb=new StringBuffer();
            String label=null;
            if(type == Neo4jEnum.RELATE.getValue()){
                for(String k:keys){
                    if(first && !k.contains(endId) && !k.contains(startId)){
                        sb.append(",");
                    }

                    if(k.contains(startId)){
                        label = BaseUtil.getBrackets(k);
                    }else if(!k.contains(endId)){
                        sb.append(k+":{"+k+"}");
                        first =true;
                    }
                }
                if(BaseUtil.isNotBlank(label)){
                    String result="MATCH (a:"+label+"),(b:"+label+") WHERE a.id={start_id} AND b.id={end_id} " +
                            " CREATE (a)-[r:%s {"+sb.toString()+"}] ->(b)";
                    dsl = result;
                }
            }else if(type == Neo4jEnum.SAVE.getValue()){
                for(String k:keys){
                    if(first){
                        sb.append(",");
                    }
                    first =true;
                    if(k.contains(id)){
                        label = BaseUtil.getBrackets(k);
                        sb.append("id:{id}");
                    }else{
                        sb.append(k+":{"+k+"}");
                    }
                }
                if(BaseUtil.isNotBlank(label)){
                    String format="CREATE (a:%s {%s})";
                    dsl = String.format(format,label,sb.toString());
                }

            }
        }
    }

    public void toDSL(String format){
        if(BaseUtil.isNotBlank(format)){
            setAttribute(format);
            options();
            createDSL();
        }
    }

}
