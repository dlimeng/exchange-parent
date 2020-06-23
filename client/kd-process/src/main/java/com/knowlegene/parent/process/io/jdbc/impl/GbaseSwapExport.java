package com.knowlegene.parent.process.io.jdbc.impl;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.scheduler.utils.CacheManager;

import javax.sql.DataSource;

/**
 * @Classname GbaseSwapExport
 * @Description TODO
 * @Date 2020/6/23 10:40
 * @Created by limeng
 */
public class GbaseSwapExport extends GbaseSwapImpl {
    @Override
    public DataSource getDataSource() {
        String name = DBOperationEnum.GBASE_EXPORT_DB.getName();
        if(CacheManager.isExist(name)){
            return (DataSource)CacheManager.getCache(name);
        }
        return null;
    }
}
