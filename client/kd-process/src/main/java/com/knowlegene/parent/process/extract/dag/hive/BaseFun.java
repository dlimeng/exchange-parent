package com.knowlegene.parent.process.extract.dag.hive;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.HSqlThreadLocalUtil;
import com.knowlegene.parent.process.pojo.NestingFields;
import com.knowlegene.parent.process.transform.common.BaseTransform;
import com.knowlegene.parent.process.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 基础函数
 * @Author: limeng
 * @Date: 2019/8/28 16:42
 */
public abstract class BaseFun extends BaseTransform implements BaseETLDefaultMethod {
    private final Logger logger;

    public BaseFun() {
        this.logger = LoggerFactory.getLogger(getClass());
    }

    public Logger getLogger() {
        return logger;
    }
    public abstract List<String> getConfig();
    public abstract void customizeFun();

    /**
     * hive
     * 两个版本根据标识字段合并表，生成结果表
     * @param oldTable 老版本表
     * @param newTable 新版本表
     * @param mark 标识字段
     * @return
     */
    public String mergeTable(String oldTable,String newTable,String mark){
        return this.getHiveFun().mergeField2(oldTable,newTable,mark);
    }

    /**
     * hive嵌套字段
     * @param tableName 表名称
     * @param keys 根据keys合并，名称
     * @param columns 普通字段名称
     * @param nestings 合并字段名称 key 嵌套字段名称，string[] 合并字段名称
     * @return 转存表名  tableName_to_es
     */
    public String nestingTable(String tableName,String[] keys,String[] columns,Map<String,String[]> nestings){
        NestingFields nestingFields = new NestingFields();
        nestingFields.setColumns(columns);
        nestingFields.setTableName(tableName);
        nestingFields.setNestings(nestings);
        nestingFields.setKeys(keys);
        return this.getHiveFun().nestingFieldToEs(nestingFields);
    }

    /**
     * hive嵌套字段
     * @param tableName 表名称
     * @param keys 根据keys合并，名称
     * @param nestings 合并字段名称
     * @return 返回值
     */
    public String nestingTable(String tableName,String[] keys,Map<String,String[]> nestings){
        return nestingTable(tableName,keys,null,nestings);
    }

    @Override
    public void etl() {
        List<String> config = getConfig();
        if(!BaseUtil.isBlankSet(config)){
            HSqlThreadLocalUtil.clearJob();
            SqlUtil.setHiveVariable(config);
        }
        customizeFun();
    }

    @Override
    public void ddl() {

    }
}
