package com.cg.javacore.reflect;

public class HelloWorld {
	public String str=null;
	private int id=0;
	
	public HelloWorld(){
		
	}
	
	public HelloWorld(String str){
		this.str=str;
	}
	
	public void sayHello() {
		System.out.println("Hello");
	}

	public void sayYes() {
		System.out.println("Yes");
	}
	
	public static void main(String[] args){
		for (String string : args) {
			System.out.println(string);
		}
	}
}
