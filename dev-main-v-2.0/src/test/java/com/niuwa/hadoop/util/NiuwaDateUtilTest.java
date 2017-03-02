package com.niuwa.hadoop.util;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.junit.Test;

import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;

public class NiuwaDateUtilTest {
	@Test
	public void defaultDateTest() throws ParseException{
		Calendar firstDayOfCurrentMonth= Calendar.getInstance();
		firstDayOfCurrentMonth.setTime(ChubaoDateUtil.getFirstDayOfMonth());

		ChubaoDateUtil.setDataLastedTime();

		assertEquals(firstDayOfCurrentMonth, ChubaoDateUtil.dataLastedTime);

		ChubaoDateUtil.setDataLastedTime(DateUtil.parse("2015-05-05"));
		
		firstDayOfCurrentMonth.setTime(new SimpleDateFormat("yyyy-MM-dd").parse("2015-05-05"));
		
		assertEquals(firstDayOfCurrentMonth, ChubaoDateUtil.dataLastedTime);
		
	}
}
