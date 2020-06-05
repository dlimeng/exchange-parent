package com.knowlegene.parent.cetl.hivemart.relationetl;

import com.knowlegene.parent.config.pojo.sql.SQLResult;
import com.knowlegene.parent.process.extract.dag.hive.BaseDDL;

import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/14 15:25
 */
public class RelationDDL extends BaseDDL {
    @Override
    public List<String> getResults() {
        String path="/relation-ddl.sql";
        return this.getExtractFile().readStreamPath(path);
    }

    @Override
    public SQLResult getResult() {
        return null;
    }
}
