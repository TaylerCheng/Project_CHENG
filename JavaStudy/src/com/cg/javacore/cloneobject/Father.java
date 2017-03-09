package com.cg.javacore.cloneobject;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;

@JsonIgnoreProperties(ignoreUnknown = true)
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

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return name;
	}
}
