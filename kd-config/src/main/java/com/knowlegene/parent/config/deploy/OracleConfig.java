package com.knowlegene.parent.config.deploy;

import com.knowlegene.parent.config.util.PropertiesUtil;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Author: limeng
 * @Date: 2019/7/22 20:41
 */
@Data
public class OracleConfig {
    private Logger logger = LoggerFactory.getLogger(HiveConfig.class);

    private String className;
    private String url;
    private String  user;
    private String  password;
    /**
     * 连接池保持的最小连接数
     */
    private Integer minPoolSize;

    /**
     * 连接池在无空闲连接可用时一次性创建的新数据库连接数
     */
    private Integer acquireIncrement;
    /**
     * 最大连接数
     */
    private Integer maxPoolSize;

    /**
     * 连接的最大空闲时间
     */
    private Integer maxIdleTime;



    public Map<String,DataSource> getOracleDataSourceMap(){
        Map<String,DataSource> hiveDataSourceMap = new HashMap<>();
        hiveDataSourceMap.put("o1",getOracleDataSource1());
        return hiveDataSourceMap;
    }

    public DataSource getOracleDataSource1(){
        Properties properties = PropertiesUtil.getProperties();
        className = properties.getProperty("kd.oracle.classname");
        url=properties.getProperty("kd.oracle.url");
        password=properties.getProperty("kd.oracle.password");
        user=properties.getProperty("kd.oracle.user");
        minPoolSize=Integer.parseInt(properties.getProperty("kd.oracle.minPoolSize"));
        acquireIncrement=Integer.parseInt(properties.getProperty("kd.oracle.acquireIncrement"));
        maxPoolSize=Integer.parseInt(properties.getProperty("kd.oracle.maxPoolSize"));
        maxIdleTime=Integer.parseInt(properties.getProperty("kd.oracle.maxIdleTime"));
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setUser(user);
        try {
            cpds.setDriverClass(className);
            cpds.setPassword(password);
            cpds.setJdbcUrl(url);
            cpds.setMaxPoolSize(maxPoolSize);
            cpds.setMinPoolSize(minPoolSize);
            cpds.setAcquireIncrement(acquireIncrement);
            cpds.setMaxIdleTime(maxIdleTime);
        } catch (PropertyVetoException e) {
            logger.error("getOracleDataSource1  error=>msg:{}", e.getMessage());
            return null;
        }
        return cpds;
    }
}
