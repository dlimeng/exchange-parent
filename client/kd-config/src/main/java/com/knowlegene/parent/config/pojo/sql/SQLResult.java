package com.knowlegene.parent.config.pojo.sql;

import com.knowlegene.parent.config.pojo.BaseDO;
import lombok.Data;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/8 19:06
 */
@Data
public class SQLResult extends BaseDO{
    /**
     * 总况是否有序
     */
    private Boolean isOrder;
    /**
     * 定义语句
     * 插入语句
     * 容器
     */
    private List<SQLOperators> insertOperator = new ArrayList<>();
    private List<SQLOperators> ddlOperator = new ArrayList<>();


    public List<String> getList(){
        int count = insertOperator.size() + ddlOperator.size();
        List<String> sqls = null;
        if(count >= 1){
            List<String> insertOperators = this.getSqlList(insertOperator);
            List<String> ddlOperators = this.getSqlList(ddlOperator);
            sqls = new ArrayList<>();
            sqls.addAll(insertOperators);
            sqls.addAll(ddlOperators);
        }
        return sqls;
    }

    /**
     * 获取插入sql
     * @return
     */
    public List<String> getInserts(){
        List<String> sqls = null;
        if (!CollectionUtils.isEmpty(insertOperator)) {
            sqls = this.getSqlList(insertOperator);
        }
        return sqls;
    }

    /**
     * 获取ddl
     * @return
     */
    public List<String> getDdls(){
        List<String> sqls = null;
        if (!CollectionUtils.isEmpty(ddlOperator)) {
            sqls = this.getSqlList(ddlOperator);
        }
        return sqls;
    }
    private List<String> getSqlList(List<SQLOperators> list){
        List<String> sqls = null;
        if (!CollectionUtils.isEmpty(list)) {
            sqls = new ArrayList<>();
            for(SQLOperators operatorDO : list){
                String sql = operatorDO.toString();
                sqls.add(sql);
            }
        }
        return sqls;
    }

    /**
     * 插入
     *
     */
    public SQLResult insert(SQLOperators operator){
        if(operator!=null){
            insertOperator.add(operator);
        }
        return this;
    }

    /**
     * with
     * set
     * create
     * truncate
     *
     */
    public SQLResult ddl(SQLOperators operator){
        if(operator!=null){
            ddlOperator.add(operator);
        }
        return this;
    }
}
