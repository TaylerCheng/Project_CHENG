package com.cg.springstudy.quartz;

import com.cg.springstudy.quartz.job.SayHelloJob;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

/**
 * @author： Cheng Guang
 * @date： 2017/11/7.
 */
public class ScheduledServer {

    public static void main(String[] args) throws SchedulerException {
        SayHelloJob sayHelloJob = new SayHelloJob();
        sayHelloJob.setCronSchedule("0/5 * * * * ?");
        // Job Name
        String jobName = SayHelloJob.class.getSimpleName();

        // Build job detail instance
        JobDataMap jobDataMap = new JobDataMap( );
        JobDetail jobDetail = JobBuilder.newJob()
//                .withIdentity(jobName, "FirstGroup")
                .ofType(sayHelloJob.getClass())
//                .setJobData(jobDataMap)
                .build();

        // Build trigger instance
        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(jobName + "-Trigger", "FirstGroupTrigger")
                .withSchedule(CronScheduleBuilder.cronSchedule(sayHelloJob.getCronSchedule()))
                .build();

        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler( );
        scheduler.start( );

        scheduler.scheduleJob(jobDetail,trigger);

    }

}
