package com.niuwa.hadoop.chubao.utils;

import java.util.regex.Pattern;

import com.google.common.base.Joiner;

public class ChubaoUtil {
	
	/**
	 * 手机号脱敏后形式如 :
	 * 861234567****
	 * +861234567****
	 * 
	 * @param str
	 * @return
	 */
	public static boolean telVilidate(String str){
		String regex="^(\\+){0,1}861\\d{6}.{4}";
		String anotherRegex = "1\\d{6}.{4}";
		return Pattern.matches(regex, str) ||
				Pattern.matches(anotherRegex, str);
	}
	
	/**
	 * 字符串数组截取
	 * 
	 * @param src
	 * @param start
	 * @param end
	 * @return
	 */
	public static  String array2Str(String[] src,int start,int end){
		if(src.length<end){
			return null;
		}
		String c[]=new String[end-start];
		System.arraycopy(src, start, c, 0, c.length);
		String b=Joiner.on("\t").join(c);
		return b;
	}
	
	public static String array2Str(String[] src,int skip){
		return array2Str(src,skip,src.length);
	}
	
	public static double getIntAmount(double amt){
        return Math.round(amt/100)*100;
    }
	
}
