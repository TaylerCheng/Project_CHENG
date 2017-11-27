package com.cg.springstudy.bean.service.impl;

import com.cg.springstudy.bean.service.IMyApiService;
import com.cg.springstudy.bean.service.IYourApiService;
import com.cg.springstudy.dao.mybatis.mapper.UserMapper;
import com.cg.springstudy.dao.mybatis.pojo.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Random;

/**
 * @author： Cheng Guang
 * @date： 2017/6/5.
 */
@Service
public class MyApiServiceImpl implements IMyApiService {

    @Autowired
    private IYourApiService yourApiService;
    @Autowired
    private UserMapper userMapper;

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

    @Transactional
    @Override
    public void doSomeError(User user2) {
        try {
            userMapper.insertSelective(user2);
            System.out.println(10 / 0); // 引发异常
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

