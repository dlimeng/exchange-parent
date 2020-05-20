package com.knowlegene.parent.config.deploy;

import com.knowlegene.parent.config.common.constantenum.HiveTypeEnum;
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
 * @Date: 2019/7/17 17:00
 */
@Data
public class HiveConfig {

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

    private Map<String, String> configProperties=null;


    public DataSource getHiveDataSource1(){
        if(cpds == null){
            cpds = new ComboPooledDataSource();
            Properties properties = PropertiesUtil.getProperties();
            className = properties.getProperty("kd.hive.classname");
            url=properties.getProperty("kd.hive.url");
            password=properties.getProperty("kd.hive.password");
            user=properties.getProperty("kd.hive.user");
            minPoolSize=Integer.parseInt(properties.getProperty("kd.hive.minPoolSize"));
            acquireIncrement=Integer.parseInt(properties.getProperty("kd.hive.acquireIncrement"));
            maxPoolSize=Integer.parseInt(properties.getProperty("kd.hive.maxPoolSize"));
            maxIdleTime=Integer.parseInt(properties.getProperty("kd.hive.maxIdleTime"));
            cpds.setUser(user);
            try {
                cpds.setDriverClass(className);
                cpds.setPassword(password);
                cpds.setJdbcUrl(url);
                cpds.setMaxPoolSize(maxPoolSize);
                cpds.setMinPoolSize(minPoolSize);
                cpds.setAcquireIncrement(acquireIncrement);
                cpds.setMaxIdleTime(maxIdleTime);
                //连接丢失 每10秒自动检测
                cpds.setTestConnectionOnCheckin(true);
                cpds.setPreferredTestQuery("SELECT 1");
                cpds.setIdleConnectionTestPeriod(10);

            } catch (PropertyVetoException e) {
                logger.error("getHiveDataSource1  error=>msg:{}", e.getMessage());
                return null;
            }
        }
        return cpds;
    }

    public Map<String, String> getHiveMetastore(){
        if(configProperties == null){
            configProperties = new HashMap<>();
        }
        String uris = HiveTypeEnum.HCATALOGMETASTOREURIS.getName();
        String db= HiveTypeEnum.HIVEDATABASE.getName();
        Properties properties = PropertiesUtil.getProperties();
        String dbValue = properties.getProperty("kd.hive.database");
        String urisValue = properties.getProperty("kd.hive.metastore.uris");
        configProperties.put(uris,urisValue);
        configProperties.put(db,dbValue);
        return configProperties;
    }

}
