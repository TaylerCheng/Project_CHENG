package com.cg.springstudy.service.impl;

import com.cg.springstudy.service.IMyApiService;
import com.cg.springstudy.service.IYourApiService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Random;

/**
 * @author： Cheng Guang
 * @date： 2017/6/5.
 */
@Service
public class MyApiServiceImpl implements IMyApiService {

    @Autowired
    IYourApiService yourApiService;

    @Override
    public void sayHello(int i) throws InterruptedException {
        if (i>2){
            asyncSayHello(i);
        }else {
            System.out.println("SayHello " + i);
        }
    }

    @Async
    @Override
    public void asyncSayHello(int i) throws InterruptedException {
        int randomInt = new Random().nextInt(5);
        Thread.currentThread().sleep(randomInt*1000);
        System.out.println("AsyncSayHello " + i);
//        sayHello(i);
    }

}

