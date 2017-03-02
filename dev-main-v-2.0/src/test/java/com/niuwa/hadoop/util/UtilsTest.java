package com.niuwa.hadoop.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.niuwa.hadoop.chubao.utils.ChubaoUtil;

public class UtilsTest {
	@Test
	public void testPhoneString(){
		String phone1= "+8618757120000";
		String phone2= "+861875712****";
		String phone3= "861875712****";
		
		assertEquals(ChubaoUtil.telVilidate(phone1), true);
		assertEquals(ChubaoUtil.telVilidate(phone2), true);
		assertEquals(ChubaoUtil.telVilidate(phone3), true);
	}
}
