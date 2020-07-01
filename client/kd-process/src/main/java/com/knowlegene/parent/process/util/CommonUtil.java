package com.knowlegene.parent.process.util;

import com.knowlegene.parent.config.util.BaseUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.DigestUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/9/17 16:53
 */
public class CommonUtil {
    private static Logger logger = LoggerFactory.getLogger(CommonUtil.class);

    /**
     * 注：\n 回车(\u000a) \t 水平制表符(\u0009) \s 空格(\u0008) \r 换行(\u000d)
     */
    private static  String REX="\\s|\t|\r|\n";
    /**
     * 字符串密码
     * @return
     */
    public static String encryptStr(String str) {
        String result = "";
        if (BaseUtil.isNotBlank(str)) {
            try {
                result = DigestUtils.md5DigestAsHex(str.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return result;
    }

    /**
     * 解析成neo4j cypher语义
     * @param fromat
     * @return
     */
    public static String getCypher(String fromat){
        String result=null;
        if(BaseUtil.isNotBlank(fromat)){
            fromat.split(REX);
        }
        return null;
    }

    public static List<String> getNeoParkeys(){
        return null;
    }



    public static Object matchPrimitiveType(Object value, Schema.TypeName type){
        Object result =value;

        switch (type) {
            case BYTE:
                if (value instanceof Byte) {
                    return value;
                }
                break;
            case BYTES:
                if (value instanceof ByteBuffer) {
                    return ((ByteBuffer) value).array();
                } else if (value instanceof byte[]) {
                    return (byte[]) value;
                }
                break;
            case INT16:
                if (value instanceof Short) {
                    return value;
                }
                break;
            case INT32:
                if (value instanceof Integer) {
                    return value;
                }
                break;
            case INT64:
                if (value instanceof Long) {
                    return value;
                }
                break;
            case DECIMAL:
                if (value instanceof BigDecimal) {
                    return value;
                }
                break;
            case FLOAT:
                if (value instanceof Float) {
                    return value;
                }
                break;
            case DOUBLE:
                if (value instanceof Double) {
                    return value;
                }
                break;
            case STRING:
                if (value instanceof String) {
                    return value;
                }
                break;
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                break;
            case DATETIME:
                break;
        }
        return null;
    }
}
