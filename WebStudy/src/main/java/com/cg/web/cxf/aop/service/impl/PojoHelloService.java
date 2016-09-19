package com.cg.web.cxf.aop.service.impl;

/**
 * Created by Cheng Guang on 2016/9/18.
 */
public class PojoHelloService {

    public void sayHelloToSomeBody(String name) {
        System.out.println("Hello," + name);
    }

    public void sayHello() {
        System.out.println("Hello!!!");
    }

    public void sayBye() {
        System.out.println("sayBye");
    }
}
