package com.cg.web.dubbo;

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
    public CustomerService customerService;

    @Autowired
    public LoanService loanService;

    @Test
    public void dubboTest() throws InterruptedException {
        while (true) {
//            System.out.println(customerService.getClass());
            String result = customerService.getName("Cheng");
            System.out.println(result);

            System.out.println(loanService.getApplCde());

            Thread.currentThread().sleep(1000);
        }
    }

}
