package com.cg.javacore.reflect;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class Test {

	public static void main(String[] args) throws Exception {
		HelloWorld gmv = new HelloWorld("cheng");
		
		
		//Class c1 = HelloWorld.class;
		Class c2 = Class.forName("com.cg.HelloWorld");
		Method method=c2.getMethod("main",String[].class);
		method.invoke(null, (Object)new String[]{"a","b"});
		
//		Field[] fields = c2.getDeclaredFields();
//		fields[1].setAccessible(true);
//		System.out.println(fields[1].get(gmv));

	    //Field fieldx = c2.getField("str");
		//对于静态变量，可以传入null
		//System.out.println(fieldx.get(gmv));
		
//		System.out.println(c.getConstructor(String.class));
//		Constructor[] constructors = c.getConstructors();
//		for (Constructor constructor : constructors) {
//			System.out.println(constructor);
//		}
//		HelloWorld helloWorld = new HelloWorld("cheng");
//		Field fieldx = c.getClass().getField("str");
//		
//		System.out.println(fieldx);
		//HelloWorld gmv = (HelloWorld)c.newInstance();
        
		
//        Method m[] = c.getDeclaredMethods();                  
//        for (int i = 0; i < m.length; i++){  
//            if (m[i].toString().contains("Hello")) {
//            	m[i].invoke(gmv, null);
//			}        	
//        }  
		
//		
//		gmv.sayHello();
		
	}
}
