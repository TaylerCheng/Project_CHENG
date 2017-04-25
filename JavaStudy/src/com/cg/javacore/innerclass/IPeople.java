package com.cg.javacore.innerclass;

/**
 * @author： Cheng Guang
 * @date： 2017/4/13.
 */
public interface IPeople {

    String str = "IPeople";

    default void sayHello2(){
        System.out.println(str);
    }
}
