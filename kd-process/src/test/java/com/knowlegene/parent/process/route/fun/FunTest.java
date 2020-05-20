package com.knowlegene.parent.process.route.fun;

import com.knowlegene.parent.process.extract.dag.hive.BaseFun;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/9/5 18:33
 */
public class FunTest extends BaseFun {
    @Override
    public List<String> getConfig() {
        return null;
    }

    @Override
    public void customizeFun() {
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
