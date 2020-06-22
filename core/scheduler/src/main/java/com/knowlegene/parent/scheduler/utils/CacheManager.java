package com.knowlegene.parent.scheduler.utils;


import com.knowlegene.parent.scheduler.pojo.CacheEntry;


import java.util.List;
import java.util.Map;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Classname CacheManager
 * @Description TODO
 * @Date 2020/6/9 15:24
 * @Created by limeng
 */
public class CacheManager {


    private CacheManager(){

    }
    /**
     * 当前缓存个数
     */
    private static int CURRENT_SIZE = 0;

    //是否开启清除缓存
    private static volatile Boolean clearExpireCacheEnable = true;
    //缓存失效时间
    private static final long cacheTimeout =  12 * 60 * 60 * 1000L;
    //缓存使用记录
    private final static LinkedList<Object> cacheUseRecord = new LinkedList<>();

    private final static  int MAX_CACHE_SIZE = 80;


    private final  static Map<Object, CacheEntry> cacheEntryMap =new ConcurrentHashMap<>();

    private static class  CacheManagerFactory{
        private static final CacheManager CACHE_MANAGER = new CacheManager();
    }

    private static CacheManager getCacheManagerInstance(){
        return CacheManagerFactory.CACHE_MANAGER;
    }

    /**
     * 设置缓存
     */
    public static void setCache(String cacheKey, Object cacheValue) {
        setCache(cacheKey, cacheValue, -1L);
    }


    public static void setCache(String cacheKey, Object cacheValue, long cacheTime){
        Long ttlTime = null;
        if (cacheTime <= 0L) {
            ttlTime = -1L;
        }
        checkSize();
        saveCacheUseLog(cacheKey);
        CURRENT_SIZE = CURRENT_SIZE + 1;
        if(ttlTime == null){
            ttlTime = System.currentTimeMillis() + cacheTime;
        }
        CacheEntry cacheObj = new CacheEntry(cacheValue, ttlTime);
        cacheEntryMap.put(cacheKey, cacheObj);
    }


    public static Object getCache(String cacheKey) {
        startCleanThread();
        if(checkCache(cacheKey)){
            saveCacheUseLog(cacheKey);
            return cacheEntryMap.get(cacheKey).getCacheValue();
        }
        return null;
    }


    public static boolean isExist(String cacheKey) {
        return checkCache(cacheKey);
    }
    /**
     * 判断缓存存在不存
     * @param cacheKey
     * @return
     */
    private static boolean checkCache(String cacheKey){
        CacheEntry cacheEntry = cacheEntryMap.get(cacheKey);
        if(cacheEntry == null){
            return false;
        }

        if(cacheEntry.getTtlTime() == -1L){
            return true;
        }

        if(cacheEntry.getTtlTime() <System.currentTimeMillis()){
            deleteCache(cacheKey);
            return false;
        }

        return true;
    }


    /**
     * 保存缓存使用记录
     */
    private static  synchronized void saveCacheUseLog(String cacheKey){
       synchronized (cacheUseRecord){
           cacheUseRecord.remove(cacheKey);
           cacheUseRecord.add(0,cacheKey);
       }
    }

    /**
     * 检查大小
     * 当前大小
     * 首先删除过期缓存，如果过期缓存删除过后还是达到最大缓存数目
     * 删除最久未使用缓存
     */
    private static void checkSize(){
        if(CURRENT_SIZE >= MAX_CACHE_SIZE){
            deleteTimeOut();
        }
        if (CURRENT_SIZE >= MAX_CACHE_SIZE) {
            deleteLRU();
        }

    }


    static void setCleanThreadRun(){
        clearExpireCacheEnable = false;
    }


    public static void clear(){
        cacheEntryMap.clear();
        CURRENT_SIZE = 0;
    }

    private static void deleteCache(Object cacheKey){
        Object cacheValue  = cacheEntryMap.remove(cacheKey);
        if(cacheValue != null){
            CURRENT_SIZE = CURRENT_SIZE -1;
        }
    }

    static void deleteTimeOut(){
        List<Object> deleteKeyList = new LinkedList<>();
        for(Map.Entry<Object,CacheEntry> entry:cacheEntryMap.entrySet()){
            if(entry.getValue().getTtlTime() < System.currentTimeMillis() && entry.getValue().getTtlTime() != -1L){
                deleteKeyList.add(entry.getKey());
            }
        }

        for(Object deleteKey:deleteKeyList){
            deleteCache(deleteKey);
        }
    }



    private static void startCleanThread(){
        if(clearExpireCacheEnable){
            ClearCacheTask task= new ClearCacheTask();
            Thread thread = new Thread(task);
            thread.setDaemon(true);
            thread.start();
        }
    }

    private static void deleteLRU(){
        Object cacheKey = null;
        synchronized (cacheUseRecord){
            if(CURRENT_SIZE >= MAX_CACHE_SIZE -10){
                cacheKey  = cacheUseRecord.remove(CURRENT_SIZE-1);
            }
            if(cacheKey != null){
                deleteCache(cacheKey);
            }
        }

    }


    private static class ClearCacheTask implements Runnable{

        @Override
        public void run() {
            CacheManager.setCleanThreadRun();
            while (true){
                CacheManager.deleteTimeOut();
                try {
                    Thread.sleep(CacheManager.cacheTimeout);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }


}


