package com.knowlegene.parent.process.util;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.common.constantenum.DataSourceEnum;
import com.knowlegene.parent.process.model.SwapOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/9/23 17:58
 */
public class Neo4jDataSourceUtil {
    private final static Map<String,Object> datasourceMap =new HashMap<>();
    private static Logger logger = LoggerFactory.getLogger(Neo4jDataSourceUtil.class);
    public Neo4jDataSourceUtil() {
    }
    private Neo4jDataSourceUtil(SwapOptions swapOptions) {
        if(swapOptions != null){
            String neoUrl = swapOptions.getNeoUrl();
            String neoUsername = swapOptions.getNeoUsername();
            String neoPassword = swapOptions.getNeoPassword();
            datasourceMap.put(DataSourceEnum.NEOURL.getName(),neoUrl);
            datasourceMap.put(DataSourceEnum.NEOUSERNAME.getName(),neoUsername);
            datasourceMap.put(DataSourceEnum.NEOPASSWORD.getName(),neoPassword);
        }else{
            logger.error("swapOptions is null");
        }
    }

    private static volatile Neo4jDataSourceUtil sFactory = null;

    public static Neo4jDataSourceUtil getSessionFactoryInstance(SwapOptions swapOptions) {
        if (sFactory == null) {
            sFactory = new Neo4jDataSourceUtil(swapOptions);
        }
        return sFactory;
    }

    public static Object get(String key){
        if(BaseUtil.isNotBlank(key)){
            return datasourceMap.get(key);
        }else{
            logger.warn("key is null");
        }
        return null;
    }
}
