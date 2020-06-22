package com.knowlegene.parent.scheduler.service;

import com.knowlegene.parent.scheduler.utils.CacheManager;

/**
 * @Classname TestCacheManager
 * @Description TODO
 * @Date 2020/6/10 13:58
 * @Created by limeng
 */
public class TestCacheManager {
    public static void main(String[] args) {

        CacheManager.setCache("test","value");
        CacheManager.setCache("test2","value2");
        CacheManager.setCache("test3","value4");

        Object value = CacheManager.getCache("test");
        System.out.println(value);
    }
}
