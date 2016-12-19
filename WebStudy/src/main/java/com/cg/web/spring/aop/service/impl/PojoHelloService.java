package com.cg.web.spring.aop.service.impl;

import org.springframework.stereotype.Component;

/**
 * Created by Cheng Guang on 2016/9/18.
 */
@Component
public class PojoHelloService {

    public void sayHelloToSomeBody(String name) {
        System.out.println("Hello," + name);
    }

    public void sayPojoHello() {
        System.out.println("Hello,I'm a pojo!!!");
    }

}
