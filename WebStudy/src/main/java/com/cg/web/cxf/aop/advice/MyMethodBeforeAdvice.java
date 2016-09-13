package com.cg.web.cxf.aop.advice;

import org.springframework.aop.MethodBeforeAdvice;

import java.lang.reflect.Method;

/**
 * Created by Cheng Guang on 2016/9/12.
 */
public class MyMethodBeforeAdvice implements MethodBeforeAdvice {

    @Override
    public void before(Method method, Object[] objects, Object o) throws Throwable {
        System.out.println("在方法调用前干些什么 method = " + method);
    }
}
