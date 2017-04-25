package com.cg.javacore.innerclass;

/**
 * @author： Cheng Guang
 * @date： 2017/4/13.
 */
public interface IPerson {

    public default void sayHello(){
        System.out.println("IPerson say Hello");
    }
    public default void sayHello3(){
        System.out.println("IPerson say sayHello3");
    }
}
