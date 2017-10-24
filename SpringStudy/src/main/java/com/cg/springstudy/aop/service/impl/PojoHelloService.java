package com.cg.springstudy.aop.service.impl;

import com.cg.springstudy.bean.SpringHelloWorld;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by Cheng Guang on 2016/9/18.
 */
@Component
public class PojoHelloService {

    @Autowired
    private SpringHelloWorld springHelloWorld;

    public SpringHelloWorld getSpringHelloWorld() {
        return springHelloWorld;
    }

    public void sayHelloToSomeBody(String name) {
        System.out.println("Hello," + name);
    }

    public void sayPojoHello() {
        System.out.println("Hello,I'm a pojo!!!");
    }

}
