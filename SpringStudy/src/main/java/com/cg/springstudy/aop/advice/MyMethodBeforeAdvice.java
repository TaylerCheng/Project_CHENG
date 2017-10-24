package com.cg.springstudy.aop.advice;

import org.springframework.aop.MethodBeforeAdvice;

import java.lang.reflect.Method;

/**
 * Created by Cheng Guang on 2016/9/12.
 */
public class MyMethodBeforeAdvice implements MethodBeforeAdvice {

    @Override
    public void before(Method method, Object[] objects, Object o) throws Throwable {
        System.out.println("前置通知 method = " + method.getName());
    }

    public void beforeHello() throws Throwable {
        System.out.println("我正在使用spring-aop标签实现AOP哦！");
    }

    public void beforeName(String name) throws Throwable {
        System.out.println("你想和谁打招呼啊，name = " + name);
    }
}
