package com.knowlegene.parent.process.io.es;

import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;

/**
 * es数据交换
 * @Author: limeng
 * @Date: 2019/9/9 18:40
 */
public interface ESSwap {

    ElasticsearchIO.Read getRead(String[] addresses, String index, String type);

    ElasticsearchIO.Write getWrite(String[] addresses, String index, String type);

    ElasticsearchIO.Write getWrite(String[] addresses, String index, String type, String idName);
}
