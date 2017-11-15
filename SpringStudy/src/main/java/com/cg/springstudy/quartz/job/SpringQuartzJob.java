package com.cg.springstudy.quartz.job;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import java.util.Date;

/**
 * @author： Cheng Guang
 * @date： 2017/11/7.
 */
public class SpringQuartzJob extends QuartzJobBean {

    @Override
    protected void executeInternal(JobExecutionContext jobContext) throws JobExecutionException {
        System.out.println("现在时间为：" + new Date());
        //可以通过上下文获取到JobDataMap，这里面可以存放一些参数类型的数据
        JobDataMap dataMap = jobContext.getMergedJobDataMap();
        String wish = (String) dataMap.get("wish");
        System.out.println(wish);
    }

}
