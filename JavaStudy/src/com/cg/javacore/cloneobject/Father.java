package com.cg.javacore.cloneobject;

import java.util.ArrayList;

public class Father  {
	String name;

	public Father() {
		this("cheng");
	}

	public Father(String name) {
		this.name=name;
	}

    protected void add(ArrayList<String> list) {
	
	}
	
    private void sayYes() {
		System.out.println("yes");
	}
    
	@Override
	public String toString() {
		return name;
	}
}
