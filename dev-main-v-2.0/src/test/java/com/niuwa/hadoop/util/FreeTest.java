package com.niuwa.hadoop.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;

public class FreeTest {
	public static void main(String[] args) throws ParseException{
		ChubaoDateUtil.setDataLastedTime(DateUtil.parse("2017-10-21"));
		
		SimpleDateFormat dateString= new 	SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		System.out.println("time "+  dateString.format(ChubaoDateUtil.dataLastedTime.getTime()));		
	}
}
