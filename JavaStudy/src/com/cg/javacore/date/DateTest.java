package com.cg.javacore.date;

import java.util.Calendar;
import java.util.Date;

/**
 * @author: Cheng Guang
 * @date: 2016/11/11
 */
public class DateTest {

    public static void main(String[] args) {
//        Date d3 = new Date(1970-1900,1-1,1,8,0);
//        System.out.println(d3.getYear());
//        System.out.println(d3.getTime());
//
        Date d4 = new Date();
        System.out.println(d4);

        Calendar c1 = Calendar.getInstance();
//        c1.add(Calendar.DAY_OF_MONTH, -1);

        System.out.println(c1.getTime());


    }
}
