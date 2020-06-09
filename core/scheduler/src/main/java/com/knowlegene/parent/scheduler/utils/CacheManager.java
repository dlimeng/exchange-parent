package com.knowlegene.parent.scheduler.utils;


import com.knowlegene.parent.scheduler.pojo.CacheEntry;


import java.util.List;
import java.util.Map;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

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

    private static ReentrantReadWriteLock reentrantReadWriteLock  = new ReentrantReadWriteLock();
    private static Lock writeLock = reentrantReadWriteLock.writeLock();
    private static Lock readLock = reentrantReadWriteLock.readLock();

    private final  static Map<Object, CacheEntry> cacheEntryMap =new ConcurrentHashMap<>();

    private static class  CacheManagerFactory{
        private static final CacheManager CACHE_MANAGER = new CacheManager();
    }

    private static CacheManager getCacheManagerInstance(){
        return CacheManagerFactory.CACHE_MANAGER;
    }




    //启动清除失效缓存数据
    private void initClearTask(){
        if(clearExpireCacheEnable){

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


