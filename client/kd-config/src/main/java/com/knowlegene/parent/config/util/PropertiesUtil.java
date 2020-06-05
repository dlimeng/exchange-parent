package com.knowlegene.parent.config.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 获取参数
 * @Author: limeng
 * @Date: 2019/7/23 22:32
 */
public class PropertiesUtil {
    private static final String defaultPath="/database.properties";
    private static Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);

    private static Map<String, Properties> propertiesMap=new HashMap<>();

    public static Properties  getProperties(){
        return apply(defaultPath);
    }

    public static Properties  getProperties(String path){
        return apply(path);
    }

    private static Properties apply(String path){
        Properties properties = propertiesMap.get(path);
        if(properties == null){
            properties = new Properties();
            try {
                properties.load(PropertiesUtil.class.getResourceAsStream(path));
                propertiesMap.put(path,properties);
            } catch (IOException e) {
                logger.error("PropertiesUtil apply error=>msg:{}",e.getMessage());
            }
        }
        return properties;
    }


}
