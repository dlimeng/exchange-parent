package com.knowlegene.parent.cetl.hivemart.entityetl;

import com.knowlegene.parent.config.pojo.sql.SQLResult;
import com.knowlegene.parent.process.extract.dag.hive.BaseETL;

import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/13 20:06
 */
public class EntityETL extends BaseETL {
    @Override
    public List<String> getResults() {
        String path="/entity-etl.sql";
        return this.getExtractFile().readStreamPath(path);
    }

    @Override
    public SQLResult getResult() {
        return null;
    }
}
