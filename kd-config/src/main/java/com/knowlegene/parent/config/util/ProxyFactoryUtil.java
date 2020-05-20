package com.knowlegene.parent.config.util;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import java.lang.reflect.Method;

/**
 * 代理
 * @Author: limeng
 * @Date: 2019/7/31 18:35
 */
public class ProxyFactoryUtil implements MethodInterceptor  {
    //维护目标对象
    private Object target;

    public ProxyFactoryUtil(Object target) {
        this.target = target;
    }

    /**
     * 给目标对象创建一个代理对象
     * @return
     */
    public Object getProxyInstance(){
        //1.工具类
        Enhancer en = new Enhancer();
        //2.设置父类
        en.setSuperclass(target.getClass());
        //3.设置回调函数
        en.setCallback(this);
        //4.创建子类(代理对象)
        return en.create();

    }

    @Override
    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
        //执行目标对象的方法
        Object returnValue = method.invoke(target, objects);

        return returnValue;
    }
}
