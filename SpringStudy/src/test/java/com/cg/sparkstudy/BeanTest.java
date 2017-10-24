package com.cg.sparkstudy;

import com.cg.springstudy.aop.service.impl.PojoHelloService;
import com.cg.springstudy.bean.SpringHelloWorld;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author： Cheng Guang
 * @date： 2017/4/21.
 */
public class BeanTest {

    @Test
    public void  test001(){
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");
//        SpringHelloWorld helloWorld = (SpringHelloWorld) applicationContext.getBean("springHelloWorld");
//        SpringHelloWorld helloWorld2 = (SpringHelloWorld) applicationContext.getBean("SpringHelloWorld");
//        System.out.println(helloWorld.equals(helloWorld2));
        PojoHelloService pojoHelloService = (PojoHelloService) applicationContext.getBean("pojoHelloService");
        SpringHelloWorld springHelloWorld = pojoHelloService.getSpringHelloWorld();
        SpringHelloWorld helloWorld = (SpringHelloWorld) applicationContext.getBean("springHelloWorld");
        System.out.println(springHelloWorld==helloWorld);

    }

}
