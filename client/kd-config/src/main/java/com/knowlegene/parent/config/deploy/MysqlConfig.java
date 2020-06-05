package com.knowlegene.parent.config.deploy;

import com.knowlegene.parent.config.util.PropertiesUtil;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.util.Properties;

/**
 * @Author: limeng
 * @Date: 2019/7/22 20:41
 */
@Data
public class MysqlConfig {
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


    private ComboPooledDataSource cpds = null;
    

    public DataSource getMysqlDataSource1(){
        if(cpds == null){
            cpds = new ComboPooledDataSource();
            Properties properties = PropertiesUtil.getProperties();
            className = properties.getProperty("kd.mysql.classname");
            url=properties.getProperty("kd.mysql.url");
            password=properties.getProperty("kd.mysql.password");
            user=properties.getProperty("kd.mysql.user");
            minPoolSize=Integer.parseInt(properties.getProperty("kd.mysql.minPoolSize"));
            acquireIncrement=Integer.parseInt(properties.getProperty("kd.mysql.acquireIncrement"));
            maxPoolSize=Integer.parseInt(properties.getProperty("kd.mysql.maxPoolSize"));
            maxIdleTime=Integer.parseInt(properties.getProperty("kd.mysql.maxIdleTime"));

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
                logger.error("getMysqlDataSource1  error=>msg:{}", e.getMessage());
                return null;
            }
        }
        return cpds;
    }
}
