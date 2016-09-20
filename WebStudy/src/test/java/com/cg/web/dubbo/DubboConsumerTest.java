package com.cg.web.dubbo;

import com.cg.web.dubbo.service.CustomerService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by Cheng Guang on 2016/9/20.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:spring/spring-dubbo-consumer.xml" })
public class DubboConsumerTest {

    @Autowired
    public CustomerService demoService ;

    @Test
    public void dubboTest(){
        System.out.println(demoService.getClass());
        String result = demoService.getName("Cheng");
        System.out.println(result);
    }

}
