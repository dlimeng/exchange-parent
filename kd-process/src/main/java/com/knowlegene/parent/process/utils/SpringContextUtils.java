package com.knowlegene.parent.process.utils;

import org.springframework.context.ApplicationContextAware;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.context.ApplicationContext;

/**
 * 实现对spring context 的管理
 * @Author: limeng
 * @Date: 2019/7/15 10:09
 */
public class SpringContextUtils  implements ApplicationContextAware {
    private static ApplicationContext applicationContext;

    /**
     * 实现ApplicationContextAware接口的context注入函数, 将其存入静态变量.
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        // NOSONAR
        SpringContextUtils.applicationContext = applicationContext;
    }

    /**
     * 取得存储在静态变量中的ApplicationContext.
     */
    public static ApplicationContext getApplicationContext() {
        checkApplicationContext();
        return applicationContext;
    }

    /**
     * 从静态变量ApplicationContext中取得Bean, 自动转型为所赋值对象的类型.
     */
    @SuppressWarnings("unchecked")
    public static <T> T getBean(String name) {
        checkApplicationContext();
        return (T) applicationContext.getBean(name);
    }

    /**
     * 从静态变量ApplicationContext中取得Bean, 自动转型为所赋值对象的类型.
     */
    @SuppressWarnings("unchecked")
    public static <T> T getBean(Class<T> clazz) {
        checkApplicationContext();
        return (T) applicationContext.getBeansOfType(clazz);
    }

    /**
     * 清除applicationContext静态变量.
     */
    public static void cleanApplicationContext() {
        applicationContext = null;
    }

    private static void checkApplicationContext() {
        if (applicationContext == null) {
            throw new IllegalStateException(
                    "applicaitonContext未注入,请在applicationContext.xml中定义SpringContextUtils");
        }
    }


    public static void setHttpRequestResponseHolder(HttpServletRequest request, HttpServletResponse response){
        //responseThreadLocal.set(response);
        //ApplicationContext ap = WebApplicationContextUtils.getWebApplicationContext(null);
    }
    public static HttpServletResponse getHttpResponse(){
        return responseThreadLocal.get();
    }

    public static void clean(){
        responseThreadLocal.remove();
    }

    private static final ThreadLocal<HttpServletResponse> responseThreadLocal = new ThreadLocal();
}
