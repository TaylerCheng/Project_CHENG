package com.cg.sparkstudy;

import com.cg.springstudy.aop.service.impl.PojoHelloService;
import com.cg.springstudy.bean.SpringHelloWorld;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author： Cheng Guang
 * @date： 2017/4/21.
 */
public class BeanTest {

    ApplicationContext applicationContext;

    @Before
    public void  init(){
         applicationContext = new ClassPathXmlApplicationContext("spring/applicationContext.xml");
    }

    @Test
    public void  test001(){
        SpringHelloWorld helloWorld = (SpringHelloWorld) applicationContext.getBean("springHelloWorld");
        SpringHelloWorld helloWorld2 = (SpringHelloWorld) applicationContext.getBean("springHelloWorld");
        System.out.println(helloWorld.equals(helloWorld2));
//        PojoHelloService pojoHelloService = (PojoHelloService) applicationContext.getBean("pojoHelloService");
//        SpringHelloWorld springHelloWorld = pojoHelloService.getSpringHelloWorld();
//        SpringHelloWorld helloWorld = (SpringHelloWorld) applicationContext.getBean("springHelloWorld");
//        System.out.println(springHelloWorld==helloWorld);

    }

    @Test
    public void test002() throws InterruptedException {
        Thread.sleep(12000);
    }


}
