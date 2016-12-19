package com.cg.web.spring.aop.service.impl;

import com.cg.web.spring.aop.service.IMyHelloService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by Cheng Guang on 2016/9/12.
 */
@Service("iMyHelloService")
public class MyHelloServiceImpl implements IMyHelloService {

    @Autowired
    private PojoHelloService pojoHelloService;

    @Override
    public void sayHello() {
//        pojoHelloService.sayPojoHello();
        System.out.println("Hello World!");
    }

    @Override
    public void sayHello(String name) {
        System.out.println("Hello," + name);
    }

    @Override
    public void sayHello(Long custId, String name) {
        System.out.println("Hello," + name);
    }

    @Override
    public void sayBye() {
        System.out.println("sayBye");
    }
}
