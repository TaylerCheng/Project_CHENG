package com.cg.web.cxf.aop.service.impl;

import com.cg.web.cxf.aop.service.IMyHelloService;

/**
 * Created by Cheng Guang on 2016/9/12.
 */
public class MyHelloServiceImpl implements IMyHelloService {

    @Override
    public void sayHello() {
        System.out.println("sayHello");
    }

    @Override
    public void sayBye() {
        System.out.println("sayBye");
    }
}
