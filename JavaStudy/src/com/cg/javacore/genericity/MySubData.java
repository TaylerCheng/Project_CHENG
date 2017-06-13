package com.cg.javacore.genericity;

/**
 * @author： Cheng Guang
 * @date： 2017/5/23.
 */
public class MySubData extends  MyData<String> {

    /**
     * 编译器会生成两个方法名，一个本方法；一个桥接方法，接收参数为Object
     * @param data
     */
    @Override
    public void setData(String data) {

    }

}
