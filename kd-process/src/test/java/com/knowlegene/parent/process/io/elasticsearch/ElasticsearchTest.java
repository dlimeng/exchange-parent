package com.knowlegene.parent.process.io.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

/**
 * es测试
 * @Author: limeng
 * @Date: 2019/9/5 18:43
 */
@RunWith(JUnit4.class)
public class ElasticsearchTest {
    private static RestClient restClient;

    private static Pipeline pipeline = Pipeline.create();


    @AfterClass
    public static void  afterClass(){
        pipeline.run().waitUntilFinish();
    }

    public void testRead(){

    }
    @Test
    public void testUpdate() throws IOException {
        String[] addrs=new String[]{"http://192.168.100.102:9210","http://192.168.100.103:9210","http://192.168.100.104:9210"};
        String index="kd-test";
        String type="my-type";
        ElasticsearchIO.ConnectionConfiguration connectionConfiguration = ElasticsearchIO.ConnectionConfiguration.create(addrs, index, type);
        ElasticsearchIO.Write write = ElasticsearchIO.write().withConnectionConfiguration(connectionConfiguration).withIdFn(new FieldValueExtractFnTest());
        String dsl="{\"name\":\"nametest\"}";
        pipeline.apply(Create.of(dsl)).apply(write);
    }

    public static class FieldValueExtractFnTest implements ElasticsearchIO.Write.FieldValueExtractFn{
        @Override
        public String apply(JsonNode input) {
            String value = "2r03Hm0B3xs8YQ7DAuRT";
            return value;
        }
    }


    @Test
    public void testDelete() throws IOException {
        //String ip="192.168.100.102";
        //int port=9210;
        //RestClient restClient = RestClient.builder(new HttpHost(ip, port)).setMaxRetryTimeoutMillis(6000).build();

    }


}
