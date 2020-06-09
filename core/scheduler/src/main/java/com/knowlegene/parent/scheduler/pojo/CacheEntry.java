package com.knowlegene.parent.scheduler.pojo;

import lombok.Data;

/**
 * @Classname CacheEntry
 * @Description TODO
 * @Date 2020/6/9 15:40
 * @Created by limeng
 */
@Data
public class CacheEntry {
    private Object cacheValue;
    private Long ttlTime;

    private long defaultTime = 1000L;

    public CacheEntry(Object cacheValue) {
        this.cacheValue = cacheValue;
        this.ttlTime = defaultTime;
    }

    public CacheEntry(Object cacheValue, Long ttlTime) {
        this.cacheValue = cacheValue;
        this.ttlTime = ttlTime;
    }

    public CacheEntry() {
    }
}
