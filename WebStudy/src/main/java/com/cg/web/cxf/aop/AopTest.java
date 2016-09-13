package com.cg.web.cxf.aop;

import com.cg.web.cxf.aop.service.IMyHelloService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by Cheng Guang on 2016/9/13.
 */
public class AopTest {

    public static void main(String[] args) {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");
        IMyHelloService iMyHelloService = (IMyHelloService) applicationContext.getBean("proxyFactoryBean");
        iMyHelloService.sayHello();

    }

}
