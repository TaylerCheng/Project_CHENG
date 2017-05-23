package com.cg.javacore.genericity;

import com.alibaba.fastjson.TypeReference;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * @author： Cheng Guang
 * @date： 2017/5/23.
 */
public class GenericityMain {

    public static void main(String[] args) {
//        testList();
        testFastJson();
    }

    private static void testList() {
        //        List<Object> sList= new ArrayList<String>();
        List<? extends CharSequence> eList= new ArrayList<String>();
        //        eList.add("1");
        List<? super String> sList= new ArrayList<CharSequence>();
        //        String s = sList.get(0);
        List<?> sList2= new ArrayList<String>();
//        sList2.add(new Object());//err
        Object o = sList2.get(1);

    }

    private static void testFastJson() {
        //1、普通泛型类
        MyData<String> myData = new MyData();
        Type myDataGeneric = myData.getClass().getGenericSuperclass();
        System.out.println(myDataGeneric.getClass());
        //1.1 用子类获取泛型信息(与匿名内部类一致)
        MySubData mySubData = new MySubData();
        Type mySubDataGeneric = mySubData.getClass().getGenericSuperclass();
        System.out.println(mySubDataGeneric.getClass());

        //2、模仿FastJson泛型类（使用匿名内部类）
//        MyJsonData<MyData<String>> myJsonData = new MyJsonData<MyData<String>>(){};
//        Type myJsonDataGeneric = myJsonData.getClass().getGenericSuperclass();
//        System.out.println(myJsonDataGeneric.getClass());

        //3、FastJson
//        TypeReference<String> typeReference = new TypeReference<String>(){};
//        Type referenceGeneric = typeReference.getClass().getGenericSuperclass();
//        System.out.println(referenceGeneric.getClass());
//        Type type = ((ParameterizedType) referenceType).getActualTypeArguments()[0];

    }

}
