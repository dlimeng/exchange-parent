package com.knowlegene.parent.process.util;

import com.knowlegene.parent.config.util.BaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.DigestUtils;

import java.io.UnsupportedEncodingException;
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

}
