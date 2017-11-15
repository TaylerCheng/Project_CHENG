package com.cg.springstudy.quartz;

import com.cg.springstudy.quartz.job.SayHelloJob;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

/**
 * @author： Cheng Guang
 * @date： 2017/11/7.
 */
public class ScheduledServer {

    private static final String DEFAULT_CRON_SCHEDULE = "0/5 * * * * ?";

    public static void main(String[] args) throws SchedulerException {
        // Job Name
        String jobName = SayHelloJob.class.getSimpleName();

        // Build job detail instance
        JobDataMap jobDataMap = new JobDataMap( );
        JobDetail jobDetail = JobBuilder.newJob()
                .withIdentity(jobName, "FirstGroup")
                .ofType(SayHelloJob.class)
//                .setJobData(jobDataMap)
                .build();

        // Build trigger instance
        CronTrigger trigger = TriggerBuilder.newTrigger()
                .forJob(jobDetail)
                .withIdentity(jobName + "-Trigger", "FirstGroupTrigger")
                .withSchedule(CronScheduleBuilder.cronSchedule(DEFAULT_CRON_SCHEDULE))
                .build();

        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler( );
        scheduler.start( );

        scheduler.scheduleJob(jobDetail,trigger);
    }

}
