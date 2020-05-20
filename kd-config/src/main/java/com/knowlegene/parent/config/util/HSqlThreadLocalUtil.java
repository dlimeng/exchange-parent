package com.knowlegene.parent.config.util;

import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/15 23:15
 */
public class HSqlThreadLocalUtil {
    private static final ThreadLocal<List<String>> CONTEXT_HOLDER = new ThreadLocal<>();

    /**
     * @Description: id
     * @return void
     */
    public static void setJob(List<String> sqls) {
        CONTEXT_HOLDER.set(sqls);
    }

    /**
     * @Description: id
     */
    public static List<String> getJob() {
        return CONTEXT_HOLDER.get();
    }

    /**
     * @Description: job
     */
    public static void clearJob() {
        CONTEXT_HOLDER.remove();
    }
}
