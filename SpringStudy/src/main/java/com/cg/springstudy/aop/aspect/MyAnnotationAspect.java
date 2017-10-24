package com.cg.springstudy.aop.aspect;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

/**
 * Created by Cheng Guang on 2016/12/8.
 */
@Aspect
@Component
public class MyAnnotationAspect {

    @Pointcut("execution(* com.cg.springstudy.aop.service.impl.PojoHelloService.sayPojoHello(..))")
    public void sayHello() {

    }

    @Pointcut("execution(* com.cg.springstudy.aop.service.IMyHelloService.sayHello(..))")
    public void sayHello2() {

    }

    @Before("sayHello() or sayHello2()")
    public void beforeSayHello() {
        System.out.println("调用前置通知");
    }

}
