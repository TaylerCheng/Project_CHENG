package com.cg.dubbox.provider;

/**
 * Created by Cheng Guang on 2016/9/20.
 */

import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

public class DemoProvider {

    public static void main(String[] args) {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
                "classpath*:spring/spring-dubbox-provider.xml");
        context.start();
        System.out.println("服务已经启动...");
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
