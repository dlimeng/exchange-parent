package com.knowlegene.parent.process.io.jdbc.impl;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.scheduler.utils.CacheManager;

import javax.sql.DataSource;

/**
 * @Classname OracleSwapExport
 * @Description TODO
 * @Date 2020/6/23 10:07
 * @Created by limeng
 */
public class OracleSwapExport extends OracleSwapImpl {
    @Override
    public DataSource getDataSource() {
        String name = DBOperationEnum.ORACLE_EXPORT_DB.getName();
        if(CacheManager.isExist(name)){
            return (DataSource)CacheManager.getCache(name);
        }
        return null;
    }
}
