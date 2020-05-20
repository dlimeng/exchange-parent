package com.knowlegene.parent.process.util;

/**
 * 设置jobid
 * @Author: limeng
 * @Date: 2019/8/13 17:36
 */
public class JobThreadLocalUtil {
    private static final ThreadLocal<String> CONTEXT_HOLDER = new ThreadLocal<String>();

    /**
     * @Description: id
     * @return void
     */
    public static void setJob(String type) {
        CONTEXT_HOLDER.set(type);
    }

    /**
     * @Description: id
     */
    public static String getJob() {
        return CONTEXT_HOLDER.get();
    }

    /**
     * @Description: job
     */
    public static void clearJob() {
        CONTEXT_HOLDER.remove();
    }
}
