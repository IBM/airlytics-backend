package com.ibm.weather.airlytics.jobs.dsr;

import com.ibm.weather.airlytics.jobs.dsr.db.AthenaDao;
import com.ibm.weather.airlytics.jobs.dsr.dto.DeletedUserMarker;
import com.ibm.weather.airlytics.jobs.dsr.dto.DsrJobAirlockConfig;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.support.CronSequenceGenerator;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.TimeZone;
import static org.junit.jupiter.api.Assertions.*;

public class TestParsing {

    @Test
    public void testCron() {
        String cronExpression = "0 45 */6 * * *";
        CronSequenceGenerator generator = new CronSequenceGenerator(cronExpression, TimeZone.getTimeZone("UTC"));
        Date nextExecutionDate = generator.next(new Date());
        System.out.println(nextExecutionDate);
    }

    @Test
    public void testMarker() {
        //dataFilePrefix+File.separator+"DbDumps"+File.separator+topicFolder+File.separator+"shard="+shard+File.separator+"day="+day+File.separator+file;
        Path p = Paths.get("/base/DbDumps/MyTestApp/shard=666/day=2020-11-03/_user_test-user_day_2021-12-04");
        DeletedUserMarker expected = new DeletedUserMarker();
        expected.setMarkerPath(p);
        expected.setUserId("test-user");
        expected.setShard(666);
        expected.setDsrDay(LocalDate.of(2021,12,4));
        expected.setActivityDay(LocalDate.of(2020,11,3));

        DeletedUserMarker marker = DeletedUserMarker.fromMarkerPath(p);
        assertEquals(expected, marker);
    }
}
