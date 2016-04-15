package com.cg.test;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.cg.javacore.cloneobject.Student;

public class StudentTest extends TestCase  {

	public void testEquals() {
		Student s1= new Student("cheng");
		Student s2= new Student("cheng");
		Assert.assertEquals(s1, s2); 
	}
	
	public void testGetName() {
		Student s= new Student("cheng");
		Assert.assertEquals("cheng", s.getName());
	}
}
