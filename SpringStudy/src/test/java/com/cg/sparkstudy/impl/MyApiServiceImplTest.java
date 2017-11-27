package com.cg.sparkstudy.impl;

import com.cg.springstudy.bean.service.IMyApiService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author： Cheng Guang
 * @date： 2017/6/5.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:spring/applicationContext.xml" })
public class MyApiServiceImplTest {

   @Autowired
   private IMyApiService myApiService;

    @Test
    public void sayHello() throws Exception {
        for (int i = 1; i < 5; i++) {
            myApiService.sayHello(i);
        }
        System.out.println("主线程执行完毕");
        Thread.currentThread().sleep(5*1000);
    }

    @Test
    public void asyncSayHello() throws Exception {
        for (int i = 1; i < 5; i++) {
            myApiService.asyncSayHello(i);
        }
        System.out.println("主线程执行完毕");
        Thread.currentThread().sleep(5*1000);
    }

}