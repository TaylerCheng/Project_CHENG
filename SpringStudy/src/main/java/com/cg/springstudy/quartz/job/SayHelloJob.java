package com.cg.springstudy.quartz.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.Date;

/**
 * @author： Cheng Guang
 * @date： 2017/11/7.
 */
public class SayHelloJob implements Job {

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        System.out.println("Hello,the time now is" + new Date());
    }

}
