package com.knowlegene.parent.process.io.jdbc.impl;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.scheduler.utils.CacheManager;

import javax.sql.DataSource;

/**
 * @Classname MySQLSwapExport
 * @Description TODO
 * @Date 2020/6/19 20:37
 * @Created by limeng
 */
public class MySQLSwapExport extends MySQLSwapImpl {
    @Override
    public DataSource getDataSource() {
        String name = DBOperationEnum.MYSQL_EXPORT_DB.getName();
        if(CacheManager.isExist(name)){
            return (DataSource)CacheManager.getCache(name);
        }
        return null;
    }
}
