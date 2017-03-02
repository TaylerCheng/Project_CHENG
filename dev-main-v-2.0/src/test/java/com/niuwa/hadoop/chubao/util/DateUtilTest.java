package com.niuwa.hadoop.chubao.util;

import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;

public class DateUtilTest {
	public static void main(String[] args){
		ChubaoDateUtil.setDataLastedTime();
		
		System.out.print("default time"+ ChubaoDateUtil.dataLastedTime.toString() );
	}
}
