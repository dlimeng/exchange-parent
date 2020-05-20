package com.knowlegene.parent.config.pojo.sql;



import com.knowlegene.parent.config.pojo.BaseDO;
import com.knowlegene.parent.config.util.BaseUtil;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/8 19:26
 */
@Data
public class SQLOperators extends BaseDO implements SQLOperator {

    private List<String> queryBuilderqList = new ArrayList<>();
    private Operator operator;
    protected String dbType;
    private Boolean isOrder;
    public SQLOperators() {
    }

    public SQLOperators(Operator operator) {
        this.operator = operator;
    }

    @Override
    public List<String> listBuilders() {
        return queryBuilderqList;
    }


    public SQLOperators addInsert(String sql){
        if(BaseUtil.isNotBlank(sql)){
            queryBuilderqList.add(sql);
        }
        return this;
    }

    public SQLOperators addWhere(String sql){
        if(BaseUtil.isNotBlank(sql)){
            queryBuilderqList.add(sql);
        }
        return this;
    }

    public SQLOperators addJoin(String sql){
        if(BaseUtil.isNotBlank(sql)){
            queryBuilderqList.add(sql);
        }
        return this;
    }

    public SQLOperators addParent(String sql){
        if(BaseUtil.isNotBlank(sql)){
            queryBuilderqList.add(sql);
        }
        return this;
    }

    public SQLOperators addFrom(String sql){
        if(BaseUtil.isNotBlank(sql)){
            queryBuilderqList.add(sql);
        }
        return this;
    }

    public SQLOperators addSelect(String sql){
        if(BaseUtil.isNotBlank(sql)){
            queryBuilderqList.add(sql);
        }
        return this;
    }

    public SQLOperators addDDL(String sql){
        if(BaseUtil.isNotBlank(sql)){
            queryBuilderqList.add(sql);
        }
        return this;
    }

    @Override
    public String toString() {
        if(!BaseUtil.isBlankSet(queryBuilderqList)){
            StringBuffer sb=new StringBuffer();
            for (String sql:queryBuilderqList) {
                sb.append(sql+" ");
            }
            return sb.toString();
        }
        return null;
    }
}
