package com.cg.springstudy.bean;

import org.springframework.stereotype.Service;

/**
 * @author： Cheng Guang
 * @date： 2017/4/21.
 */
@Service
public class SpringHelloWorld {

    //    {
    //        System.out.println("static");
    //        System.out.println(hashCode());
    //    }

    public void sayHello() {
        System.out.println("Hello World!");
    }

}
