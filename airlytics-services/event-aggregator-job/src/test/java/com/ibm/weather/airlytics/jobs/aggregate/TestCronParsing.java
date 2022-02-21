package com.ibm.weather.airlytics.jobs.aggregate;

import org.junit.jupiter.api.Test;
import org.springframework.scheduling.support.CronSequenceGenerator;

import java.util.Date;
import java.util.TimeZone;

public class TestCronParsing {

    @Test
    public void test() {
        String cronExpression = "0 45 */6 * * *";
        CronSequenceGenerator generator = new CronSequenceGenerator(cronExpression, TimeZone.getTimeZone("UTC"));
        Date nextExecutionDate = generator.next(new Date());
        System.out.println(nextExecutionDate);
    }
}
