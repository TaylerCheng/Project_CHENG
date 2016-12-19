package com.cg.web.spring.aop.service;

/**
 * Created by Cheng Guang on 2016/9/12.
 */
public interface IMyHelloService {

    void sayHello();

    void sayHello(String name);

    void sayHello(Long custId, String name);

    void sayBye();

}
