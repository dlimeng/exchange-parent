package com.knowlegene.parent.process.util;

import com.knowlegene.parent.process.model.SwapOptions;

import javax.sql.DataSource;

/**
 * @Author: limeng
 * @Date: 2019/10/12 14:09
 */
public class GbaseDataSourceUtil {
    private DataSource cpds;

    private GbaseDataSourceUtil(SwapOptions swapOptions) {
        String driverClassName = swapOptions.getDriverClass();
        String username = swapOptions.getUsername();
        String password = swapOptions.getPassword();
        String usrl = swapOptions.getUrl();
        this.cpds=ComboPooledDataSourceUtil.setDataSource(driverClassName,username,password,usrl);
    }

    private GbaseDataSourceUtil(){

    }
    private static volatile GbaseDataSourceUtil sFactory = null;

    public static GbaseDataSourceUtil getSessionFactoryInstance(SwapOptions swapOptions) {
        if (sFactory == null) {
            sFactory = new GbaseDataSourceUtil(swapOptions);
        }
        return sFactory;
    }

    public static DataSource getDataSource(){
        return sFactory.cpds;
    }
}
