package com.cg;

import com.mongodb.internal.HexUtils;
import junit.framework.Assert;
import junit.framework.TestCase;

import com.cg.javacore.cloneobject.Student;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
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

	public void testUrlEncode() throws UnsupportedEncodingException {
		String s = "程";
		byte[] bytes = s.getBytes("utf-8");
		String hexStr = HexUtils.toHex(bytes);
		System.out.println(hexStr);
	}

	public void testDouble() {
		double d = (double) 0 / 0;
		Double d2= d;
		System.out.println(d2);

		if (d>0.1){
			System.out.println("yes");
		}else {
			System.out.println("no	");
		}
	}

	@Test
	public void test() {
		int i = 1;
		System.out.println(int.class);
	}
}
