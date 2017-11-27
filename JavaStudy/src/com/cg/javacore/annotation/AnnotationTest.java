package com.cg.javacore.annotation;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

/**
 * @author： Cheng Guang
 * @date： 2017/11/24.
 */
@ForceMasterDB(transactionManagerId = "123")
public class AnnotationTest {

    /**
     * 注解处理器
     */
    public static void main(String[] args) throws InvocationTargetException, IllegalAccessException {

        Class testClass = AnnotationTest.class;
        /**
         * 1、使用反射获取
         */
        if (testClass.isAnnotationPresent(ForceMasterDB.class)){
            Annotation annotation1 = testClass.getAnnotation(ForceMasterDB.class);
            Method[] methods = annotation1.annotationType().getDeclaredMethods();
            for (Method method : methods) {
                System.out.println("method:" + method);
                Annotation[] declaredAnnotations = method.getDeclaredAnnotations();
                for (Annotation declaredAnnotation : declaredAnnotations) {
                    System.out.println("-- method declaredAnnotation:" + declaredAnnotation);
                }
            }
        }

        /**
         * 2、直接获取
         */
//        ForceMasterDB annotation2 = (ForceMasterDB) testClass.getAnnotation(ForceMasterDB.class);
//        Annotation[] annotations = annotation2.getClass().getAnnotations();
//        for (Annotation annotation : annotations) {
//            System.out.println(annotation.toString());
//        }
        //        System.out.println(annotation2.transactionManagerId());


    }

}
