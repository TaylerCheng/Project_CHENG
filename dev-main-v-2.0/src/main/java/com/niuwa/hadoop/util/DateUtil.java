package com.niuwa.hadoop.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	public static final String FORMAT_YYYY_MM_DD= "yyyy-MM-dd";
	public static final String FORMAT_YYYY_MM_DD_2= "yyyyMMdd";
	public static final String FORMAT_YYYY_MM_DD_HH_MM_SS= "yyyy-MM-dd HH:mm:ss";
	public static final String FORMAT_YYYY_MM_DD_HH_MM_SS_2= "yyyy/MM/dd HH:mm:ss";
	
	/**
	 * default format to "yyyy-MM-dd"
	 * 
	 * @param date
	 * @return
	 */
    public static String format(Date date) {
        return format(date, FORMAT_YYYY_MM_DD);
    }
    
    /**
     * format date as {format}
     * 
     * @param date
     * @param format
     * @return
     */
    public static String format(Date date, String format) {
        return new SimpleDateFormat(format).format(date);
    }
    
    /**
     * parse format "yyyy-MM-dd"
     * 
     * @param dateStr
     * @return
     * @throws ParseException 
     */
    public static Date parse(String dateStr) throws ParseException{
    	return parse(dateStr, FORMAT_YYYY_MM_DD);
    }
    
    /**
     * 
     * 
     * @param dateStr
     * @param format
     * @return
     * @throws ParseException 
     */
    public static Date parse(String dateStr, String format) throws ParseException{
    	return new SimpleDateFormat(format).parse(dateStr);
    }
}
