package com.niuwa.hadoop.chubao.test;

import static org.junit.Assert.*;

import java.util.Comparator;
import java.util.TreeMap;

import org.junit.Test;

public class MapTreeTest {
	public void testcase1(){
		TreeMap<Integer,Boolean> map=new TreeMap<Integer,Boolean>(new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				return -(o2-o1);
			}				
		});
		
		map.put(4, false);
		map.put(2, false);
		map.put(6, false);
		map.put(1, true);
		map.put(2, false);
		map.put(2, false);
		map.put(2, false);
		map.put(3, false);
		
		if(map.size()>5){
			map.remove(map.lastKey());
		}
		
		int top5sum=0;
		for(Integer mapkey : map.keySet()){
			top5sum+=mapkey;
			System.out.println(""+mapkey);
		}
		assertEquals(top5sum, 18);
	}
}
