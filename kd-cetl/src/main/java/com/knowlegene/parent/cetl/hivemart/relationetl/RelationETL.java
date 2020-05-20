package com.knowlegene.parent.cetl.hivemart.relationetl;

import com.knowlegene.parent.config.pojo.sql.SQLResult;
import com.knowlegene.parent.process.extract.dag.hive.BaseETL;

import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/14 15:26
 */
public class RelationETL extends BaseETL {
    @Override
    public List<String> getResults() {
        String path="/relation-etl.sql";
        return this.getExtractFile().readStreamPath(path);
    }

    @Override
    public SQLResult getResult() {
        return null;
    }
}
