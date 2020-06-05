package com.knowlegene.parent.process.util;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.Serializable;

/**
 * @Author: limeng
 * @Date: 2019/8/21 17:55
 */
public class ComboPooledDataSourceUtil  implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(ComboPooledDataSourceUtil.class);

    public static DataSource setDataSource(String className,String user,String password,String url){
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setUser(user);
        try {
            cpds.setDriverClass(className);
            cpds.setPassword(password);
            cpds.setJdbcUrl(url);
            cpds.setMaxPoolSize(100);
            cpds.setMinPoolSize(5);
            cpds.setAcquireIncrement(5);
            cpds.setMaxIdleTime(600);
            //连接丢失 每10秒自动检测
            cpds.setTestConnectionOnCheckin(true);
            cpds.setPreferredTestQuery("SELECT 1");
            cpds.setIdleConnectionTestPeriod(10);
            return cpds;
        } catch (PropertyVetoException e) {
            logger.error("dataSource error=>msg:{}", e.getMessage());
            return null;
        }
    }
}
