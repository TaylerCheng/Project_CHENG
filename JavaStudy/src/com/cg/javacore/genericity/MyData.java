package com.cg.javacore.genericity;

import java.lang.reflect.ParameterizedType;

/**
 * @author： Cheng Guang
 * @date： 2017/5/23.
 */
public class MyData<T> {

    private T data;

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

}
