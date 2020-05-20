package com.knowlegene.parent.process.util;

import com.knowlegene.parent.process.model.SwapOptions;

import javax.sql.DataSource;
import java.io.Serializable;

/**
 * @Author: limeng
 * @Date: 2019/8/21 17:43
 */
public class MySQLDataSourceUtil implements Serializable {

    private DataSource cpds;

    private MySQLDataSourceUtil(SwapOptions swapOptions) {
        String driverClassName = swapOptions.getDriverClass();
        String username = swapOptions.getUsername();
        String password = swapOptions.getPassword();
        String usrl = swapOptions.getUrl();
        this.cpds=ComboPooledDataSourceUtil.setDataSource(driverClassName,username,password,usrl);
    }

    private MySQLDataSourceUtil(){

    }
    private static volatile MySQLDataSourceUtil sFactory = null;

    public static MySQLDataSourceUtil getSessionFactoryInstance(SwapOptions swapOptions) {
        if (sFactory == null) {
            sFactory = new MySQLDataSourceUtil(swapOptions);
        }
        return sFactory;
    }

    public static DataSource getDataSource(){
        return sFactory.cpds;
    }

}
