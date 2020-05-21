package com.knowlegene.parent.cetl.hivemart.businfoetl;

import com.knowlegene.parent.config.pojo.sql.SQLResult;
import com.knowlegene.parent.process.extract.dag.hive.BaseDDL;

import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/13 18:51
 */
public class BusInfoDDL extends BaseDDL {
    @Override
    public List<String> getResults() {
        String path="/busInfo-ddl.sql";
        return this.getExtractFile().readStreamPath(path);
    }

    @Override
    public SQLResult getResult() {
        return null;
    }
}
