package com.knowlegene.parent.process.io.jdbc.impl;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.scheduler.utils.CacheManager;

import javax.sql.DataSource;

/**
 * @Classname HiveSwapExport
 * @Description TODO
 * @Date 2020/6/11 15:23
 * @Created by limeng
 */
public class HiveSwapExport extends HiveSwapImpl {
    @Override
    public DataSource getDataSource() {
        String name = DBOperationEnum.HIVE_EXPORT_DB.getName();
        if(CacheManager.isExist(name)){
            return (DataSource)CacheManager.getCache(name);
        }
        return null;
    }
}
