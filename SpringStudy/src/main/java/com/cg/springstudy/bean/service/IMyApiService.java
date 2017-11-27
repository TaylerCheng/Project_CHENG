package com.cg.springstudy.bean.service;

import com.cg.springstudy.dao.mybatis.pojo.User;
import org.springframework.scheduling.annotation.Async;

/**
 * @author： Cheng Guang
 * @date： 2017/6/5.
 */
public interface IMyApiService {

    void sayHello(int i) throws InterruptedException;

    @Async
    void asyncSayHello(int i) throws InterruptedException;

    void doSomeError(User user2);
}
