package com.knowlegene.parent.process.io.jdbc.impl;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.scheduler.utils.CacheManager;

import javax.sql.DataSource;

/**
 * @Classname HiveSwapImport
 * @Description TODO
 * @Date 2020/6/11 15:21
 * @Created by limeng
 */
public class HiveSwapImport  extends HiveSwapImpl{

    @Override
    public DataSource getDataSource() {
        String name = DBOperationEnum.HIVE_IMPORT_DB.getName();
        if(CacheManager.isExist(name)){
            return (DataSource)CacheManager.getCache(name);
        }
        return null;
    }
}
