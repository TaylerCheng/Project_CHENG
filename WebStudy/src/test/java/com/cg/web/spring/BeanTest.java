package com.cg.web.spring;

import com.cg.web.spring.aop.service.impl.MyHelloServiceImpl;
import com.cg.web.spring.bean.SpringHelloWorld;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author： Cheng Guang
 * @date： 2017/4/21.
 */
public class BeanTest {

    @Test
    public void  test001(){
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("spring/applicationContext.xml");
        SpringHelloWorld helloWorld = (SpringHelloWorld) applicationContext.getBean("springHelloWorld");
        SpringHelloWorld helloWorld2 = (SpringHelloWorld) applicationContext.getBean("com.cg.web.spring.bean.SpringHelloWorld");
        System.out.println(helloWorld.equals(helloWorld2));
    }

}
