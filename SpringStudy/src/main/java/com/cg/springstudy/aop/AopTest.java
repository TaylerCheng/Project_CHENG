package com.cg.springstudy.aop;

import com.cg.springstudy.aop.service.IMyHelloService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by Cheng Guang on 2016/9/13.
 */
public class AopTest {

    public static void main(String[] args) {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("spring/applicationContext.xml");

        //1、使用传统代理proxyFactoryBean的方式实现AOP
//        IMyHelloService iMyHelloService = (IMyHelloService) applicationContext.getBean("proxyFactoryBean");
//        System.out.println(iMyHelloService.getClass());
//        iMyHelloService.sayPojoHello();
//        iMyHelloService.sayBye();

        /**
         *  2、使用声明式POJO切面的方式实现AOP
         *  如果被如果被通知类使用了接口，则使用普通代理技术
         *  如果被如果被通知类没有使用接口，则使用CGLIB动态代理技术
         */
//        IMyHelloService iMyHelloService = (IMyHelloService) applicationContext.getBean("iMyHelloService");
//        System.out.println(iMyHelloService.getClass());
//        iMyHelloService.sayPojoHello();
//        PojoHelloService pojoHelloService = (PojoHelloService) applicationContext.getBean("pojoHelloService");
//        System.out.println(pojoHelloService.getClass());
//        pojoHelloService.sayPojoHello();
//        pojoHelloService.sayHelloToSomeBody("cheng");

        //3、使用代理的方式实现AOP
        IMyHelloService iMyHelloService = (IMyHelloService) applicationContext.getBean("iMyHelloService");
        System.out.println(iMyHelloService.getClass());
        iMyHelloService.sayHello("cheng");
        iMyHelloService.sayHello(1L,"cheng");

//        PojoHelloService pojoHelloService = (PojoHelloService) applicationContext.getBean("pojoHelloService");
//        System.out.println(pojoHelloService.getClass());
//        pojoHelloService.sayPojoHello();
//        pojoHelloService.sayHelloToSomeBody("cheng");
    }

}
