package com.knowlegene.parent.cetl.hivemart.fun;

import com.knowlegene.parent.process.extract.dag.hive.BaseFun;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/8/29 17:38
 */
public class EntityFun extends BaseFun {
    @Override
    public List<String> getConfig() {
        return null;
    }

    @Override
    public void customizeFun() {
        /**
         * 两个版本表合并，生成结果表
         */
       // mergeTable("oldtable1","oldtable2","version");

        nestings();

    }

    /**
     * 生成嵌套json
     */
    public void nestings(){
        String table="nestings_test";
        String[] keys=new String[]{"id"};
        String[] columns=new String[]{"name"};
        Map<String,String[]> nestings=new HashMap<>();
        String[] nestingsValue=new String[]{"tag_name","tag_desc","tag_val"};
        //nestings key为结果表嵌套字段名称
        nestings.put("nestingsjson",nestingsValue);
        nestingTable(table,keys,columns,nestings);
    }
}
