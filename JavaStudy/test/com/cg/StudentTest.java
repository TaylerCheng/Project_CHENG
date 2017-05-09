package com.cg;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.cg.javacore.cloneobject.Student;

import java.util.HashMap;
import java.util.Map;

public class StudentTest extends TestCase  {

	public void testEquals() {
		Student s1= new Student("cheng");
		Student s2= new Student("cheng");
		s1 = null;
		s2 = null;
		System.out.println(s1==s2);
	}
	
	public void testGetName() {
		Map map = new HashMap();
		Student s2= (Student) map.get("a");
		System.out.println(s2);
//		Student s= new Student("cheng");
//		Assert.assertEquals("cheng", s.getName());
	}
}
