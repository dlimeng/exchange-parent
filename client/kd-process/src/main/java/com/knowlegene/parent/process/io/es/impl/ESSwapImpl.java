package com.knowlegene.parent.process.io.es.impl;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.io.es.ESSwap;
import com.knowlegene.parent.process.transform.ESTransform;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: limeng
 * @Date: 2019/9/9 20:00
 */
public class ESSwapImpl implements ESSwap {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private boolean isEmpty(String[] addresses, String index, String type){
        boolean result=false;
        if(addresses == null || addresses.length == 0){
            logger.error("es addresses is null");
            result = true;
        }
        if(BaseUtil.isBlank(index)){
            logger.error("es index is null");
            result = true;
        }
        if(BaseUtil.isBlank(type)){
            logger.error("es type is null");
            result = true;
        }
        return result;
    }

    /**
     * 读
     * get
     * @param addresses 地址
     * @param index
     * @param type
     * @return
     */
    @Override
    public ElasticsearchIO.Read getRead(String[] addresses, String index, String type) {
        boolean empty = isEmpty(addresses, index, type);
        if(empty) return null;
        ElasticsearchIO.ConnectionConfiguration connectionConfiguration = ElasticsearchIO.ConnectionConfiguration.create(addresses, index, type);
        return ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration).withBatchSize(10000L);
    }

    /**
     * 写
     * post
     * _bulk
     * @param addresses
     * @param index
     * @param type
     * @return
     */
    @Override
    public ElasticsearchIO.Write getWrite(String[] addresses, String index, String type) {
        boolean empty = isEmpty(addresses, index, type);
        if(empty) return null;
        ElasticsearchIO.ConnectionConfiguration connectionConfiguration = ElasticsearchIO.ConnectionConfiguration.create(addresses, index, type);
        return ElasticsearchIO.write().withConnectionConfiguration(connectionConfiguration);
    }

    @Override
    public ElasticsearchIO.Write getWrite(String[] addresses, String index, String type, String idName) {
        if(BaseUtil.isBlank(idName)){
            return null;
        }
        return getWrite(addresses,index,type).withIdFn(new ESTransform.ExtractValueFn(idName));
    }
}
