package com.niuwa.hadoop.chubao.utils;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.niuwa.hadoop.util.DateUtil;

/**
 * @author Administrator
 *
 */
public class ChubaoDateUtil {
    private static final Logger log = LoggerFactory.getLogger(ChubaoDateUtil.class);

    /**
     * 脚本启动时间点，默认到指定天的 00:00:00
     * 
     */
    public static Calendar dataLastedTime = null;

    static {
        dataLastedTime = Calendar.getInstance();
        dataLastedTime.setTime(getFirstDayOfMonth());
    }

    /**
     * 月份按30天计算
     */
    public final static int MONTH = 30;
    
    /**
     * 一天时间毫秒数
     */
    public final static double ONE_DAY_INTMILLIS = 86400.00;
    
    /**
     * 脚本启动时间点，默认到当月第一天的 00:00:00
     */
    public static void setDataLastedTime() {
        dataLastedTime = Calendar.getInstance();
        dataLastedTime.setTime(getFirstDayOfMonth());
    }

    /**
     * 根据日期字符形式初始化脚本启动时间点
     * 
     * @param dateStr
     * @throws ParseException
     */
    public static void setDataLastedTime(Date latestDate) throws ParseException {
        /*
         * DateFormat fmt =new SimpleDateFormat("yyyy-MM-dd"); Date = fmt.parse(dateStr);
         */
        dataLastedTime = Calendar.getInstance();
        dataLastedTime.setTime(latestDate);
    }

    public static void setDataLastedTime(long milliseconds) {
        dataLastedTime.setTimeInMillis(milliseconds);
    }

    /**
     * 现在时间加上月份后nowc和old时间oldc对比 如果现在时间大于等于old时间，则返回true 其他false
     * 
     * @param month
     * @param old
     * @return
     */
    public static boolean compareDateAfterMonth(int month, long old) {
        Calendar nowC = (Calendar) dataLastedTime.clone();
        nowC.add(Calendar.MONTH, month);
        Calendar oldC = Calendar.getInstance();
        oldC.setTimeInMillis(old);

        return (nowC.compareTo(oldC) > -1);
    }
    
    public static boolean isBeforeNMonths(int nMonths, long old) {
        assert(nMonths < 0);
    	Calendar nowC = (Calendar) dataLastedTime.clone();
        nowC.add(Calendar.MONTH, 0-nMonths);
        Calendar oldC = Calendar.getInstance();
        oldC.setTimeInMillis(old);

        return (nowC.compareTo(oldC) > -1);
    }
    
    
    /**是否在N个月内(两头均算)
     * @param nMonths
     * @param timeInMills
     * @return
     */
    public static boolean isInRecentNMonthsWithBothEndsIncluded(int nMonths, long timeInMills){
    	
    	assert(nMonths < 1);
    	
    	Calendar nowC = (Calendar) dataLastedTime.clone();
    	
        Calendar oldC = Calendar.getInstance();
        oldC.setTimeInMillis(timeInMills);
        
        if(nowC.compareTo(oldC) < 0){
        	//oldC为将来时间
        	return false;
        }
        
        nowC.add(Calendar.MONTH, 0 - nMonths);

        return (nowC.compareTo(oldC) < 1);
    }

    /**
     * 根据时间毫秒数返回字串日期
     * 
     * @param inMillis
     * @return
     */
    public static String getStrDate(long inMillis) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(inMillis * 1000);

        return DateUtil.format(calendar.getTime(), DateUtil.FORMAT_YYYY_MM_DD_2);
    }

    /**
     * 
     * 计算两个日期之间的间隔天数: <br>
     * 为避免出现问题，应进行四舍五入取值
     *
     * @param inMillis1
     * @param inMillis2
     * @return 相隔天数
     * @see
     * @since 2016-6-21
     */
    public static int getDateInterval(long inMillis1, long inMillis2) {
        // 得到两个日期相差的天数
        int i = 8 * 3600;//北京时间到格林尼治时间校正8小时
        double date1 = Math.ceil((inMillis1+i) / ONE_DAY_INTMILLIS);
        double date2 = Math.ceil((inMillis2+i) / ONE_DAY_INTMILLIS);
        int interval = (int) Math.abs(date1 - date2);
        return interval + 1;
    }

    /**
     * 
     * 功能描述: <br>
     * 获取当月1号0点0分的日期
     *
     * @return
     * @see [相关类/方法](可选)
     * @since [产品/模块版本](可选)
     */
    public static Date getFirstDayOfMonth() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        // 将小时至0
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        // 将分钟至0
        calendar.set(Calendar.MINUTE, 0);
        // 将秒至0
        calendar.set(Calendar.SECOND, 0);
        // 将毫秒至0
        calendar.set(Calendar.MILLISECOND, 0);
        // 获得当前月第一天
        Date sdate = calendar.getTime();
        return sdate;
    }
    
    public static void main(String[] args){
    	System.out.println(System.currentTimeMillis());
    }
}
