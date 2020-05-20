package com.knowlegene.parent.process.util;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.model.SwapOptions;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;

/**
 * @Author: limeng
 * @Date: 2019/8/21 17:44
 */
public class HiveDataSourceUtil implements Serializable {
    private  Logger logger = LoggerFactory.getLogger(this.getClass());
    public static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private DataSource cpds;

    private HiveDataSourceUtil(SwapOptions swapOptions) {
        String driverClassName = swapOptions.getHiveClass();
        if(BaseUtil.isBlank(driverClassName)){
            driverClassName = driverName;
            swapOptions.setDriverClass(driverClassName);
        }
        String username = swapOptions.getHiveUsername();
        String password = swapOptions.getHivePassword();
        String usrl = swapOptions.getHiveUrl();
        this.cpds = ComboPooledDataSourceUtil.setDataSource(driverName, username, password, usrl);
    }

    private HiveDataSourceUtil(){

    }
    private static volatile HiveDataSourceUtil sFactory = null;

    public static HiveDataSourceUtil getSessionFactoryInstance(SwapOptions swapOptions) {
        if (sFactory == null) {
            sFactory = new HiveDataSourceUtil(swapOptions);
        }
        return sFactory;
    }

    public Connection getSession() throws Exception {
        Connection connection = cpds.getConnection();
        return connection;
    }
    public static DataSource getDataSource(){
        return sFactory.cpds;
    }
}
