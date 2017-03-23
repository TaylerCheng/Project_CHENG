package com.niuwa.hadoop.chubao.utils;

import java.util.regex.Pattern;

import com.alibaba.fastjson.JSONObject;
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

	public static void removeUnusedField(JSONObject callLog) {
		callLog.remove("user_rank");//用户的触宝排名
		callLog.remove("user_is_loan");//是否在触宝有借款
		callLog.remove("user_geo");//用户地理位置
		callLog.remove("device_info");//设备信息
		callLog.remove("device_imei");//设备唯一号码
		callLog.remove("device_os");//操作系统名
		callLog.remove("device_os_version");//操作系统版本
		callLog.remove("device_manufacturer");//厂商
		callLog.remove("name");//对应demo数据中的name
		callLog.remove("call_tag");//对应demo数据中的tag
	}

}
