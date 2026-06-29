package com.powerbank.rentalservice.scheduler;

import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QuartzConfig {

    @Value("${app.rental.recurrent-interval-minutes:30}")
    private int intervalMinutes;

    @Bean
    public JobDetail recurrentPaymentJobDetail() {
        return JobBuilder.newJob(RecurrentPaymentJob.class)
                .withIdentity("recurrent-payment-job")
                .storeDurably()
                .build();
    }

    @Bean
    public Trigger recurrentPaymentTrigger(JobDetail recurrentPaymentJobDetail) {
        String cron = "0 0/" + intervalMinutes + " * * * ?";
        return TriggerBuilder.newTrigger()
                .forJob(recurrentPaymentJobDetail)
                .withIdentity("recurrent-payment-trigger")
                .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                .build();
    }
}
