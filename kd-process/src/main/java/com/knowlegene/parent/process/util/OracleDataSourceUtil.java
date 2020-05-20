package com.knowlegene.parent.process.util;

import com.knowlegene.parent.process.model.SwapOptions;

import javax.sql.DataSource;
import java.io.Serializable;

/**
 * @Author: limeng
 * @Date: 2019/8/21 17:43
 */
public class OracleDataSourceUtil implements Serializable {
    private DataSource cpds;

    private OracleDataSourceUtil(SwapOptions swapOptions) {
        String driverClassName = swapOptions.getDriverClass();
        String username = swapOptions.getUsername();
        String password = swapOptions.getPassword();
        String usrl = swapOptions.getUrl();
        this.cpds=ComboPooledDataSourceUtil.setDataSource(driverClassName,username,password,usrl);
    }

    private OracleDataSourceUtil(){

    }
    private static volatile OracleDataSourceUtil sFactory = null;

    public static OracleDataSourceUtil getSessionFactoryInstance(SwapOptions swapOptions) {
        if (sFactory == null) {
            sFactory = new OracleDataSourceUtil(swapOptions);
        }
        return sFactory;
    }

    public static DataSource getDataSource(){
        return sFactory.cpds;
    }
}
