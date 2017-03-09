package com.cg.log4j;

import com.cg.log4j.test.TestLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Cheng Guang on 2016/9/14.
 */
public class TestLog4j {

    public static Logger logger = LoggerFactory.getLogger(TestLog.class);

    public static void main(String[] args) {
        logger.info("=====================");
        System.out.println(logger.equals(TestLog.logger));
        logger.info("TestLog4j");
    }
}
