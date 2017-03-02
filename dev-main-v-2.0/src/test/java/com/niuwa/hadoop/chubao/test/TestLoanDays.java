package com.niuwa.hadoop.chubao.test;

import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;

public class TestLoanDays {
    public static void main(String[] args) {
        /**
         * 1449884737 2015/12/12 9:45:37 
         *  2015/11/30 19:58:57 
         * 1449082045 2015/12/3 2:47:25
         */
        long date1 = 1449884737;
        long date2 = 1449082045;
        long tail= date1%86400;
        long tai2= date2%86400;
        
        long i= 8*3600;
        
        long dats1 = (long) Math.ceil((date1+i) / 86400.00);
        long dats2 = (long) Math.ceil((date2+i) / 86400.00);

//        System.out.println(dats1 + "====" + dats2);
//        System.out.println(date2 / 86400.00);
//        System.out.println( Math.ceil((date2) / 86400.0) );

        int f_days = (int) Math.abs(dats1 - dats2) + 1;
        System.out.println(f_days);
        
        System.out.println(ChubaoDateUtil.getDateInterval(date1, date2));
    }
}
