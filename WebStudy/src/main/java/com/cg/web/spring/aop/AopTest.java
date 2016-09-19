package com.cg.web.spring.aop;

import com.cg.web.spring.aop.service.impl.PojoHelloService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by Cheng Guang on 2016/9/13.
 */
public class AopTest {

    public static void main(String[] args) {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");

        //1、使用代理的方式实现AOP
//        IMyHelloService iMyHelloService = (IMyHelloService) applicationContext.getBean("proxyFactoryBean");
//        iMyHelloService.sayHello();
//        iMyHelloService.sayBye();

        //2、使用声明式切面的方式实现AOP
        PojoHelloService pojoHelloService = (PojoHelloService) applicationContext.getBean("pojoHelloService");
        System.out.println(pojoHelloService.getClass());
        pojoHelloService.sayHello();
        pojoHelloService.sayHelloToSomeBody("cheng");
    }

}
