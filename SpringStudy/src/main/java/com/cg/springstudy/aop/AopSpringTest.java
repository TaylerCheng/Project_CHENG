package com.cg.springstudy.aop;

import com.cg.springstudy.aop.service.IMyHelloService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by Cheng Guang on 2016/12/8.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:spring/applicationContext.xml"})
public class AopSpringTest {
    @Autowired
    private IMyHelloService iMyHelloService;
    @Test
    public void aopTest(){
        System.out.println(iMyHelloService.getClass());
        iMyHelloService.sayHello();
    }
}
