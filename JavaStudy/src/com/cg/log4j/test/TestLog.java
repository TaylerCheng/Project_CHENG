package com.cg.log4j.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Cheng Guang on 2016/9/14.
 */
public class TestLog {

    public static Logger logger = LoggerFactory.getLogger(TestLog.class);

    public static void main(String[] args) {
        logger.info("=====================");
        logger.info("TestLog");
    }
}
