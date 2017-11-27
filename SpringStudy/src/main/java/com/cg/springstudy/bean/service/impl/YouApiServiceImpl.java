package com.cg.springstudy.bean.service.impl;

import com.cg.springstudy.bean.service.IYourApiService;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Random;

/**
 * @author： Cheng Guang
 * @date： 2017/6/5.
 */
@Service
public class YouApiServiceImpl implements IYourApiService {

    @Async
    @Override
    public void asyncSayHello(int i) throws InterruptedException {
        int randomInt = new Random().nextInt(5);
        Thread.currentThread().sleep(randomInt*1000);
        System.out.println("YouApiServiceImpl AsyncSayHello " + i);
    }

}

