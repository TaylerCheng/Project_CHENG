package com.cg.springstudy.service;

import org.springframework.scheduling.annotation.Async;

/**
 * @author： Cheng Guang
 * @date： 2017/6/5.
 */
public interface IYourApiService {

    void asyncSayHello(int i) throws InterruptedException;

}
