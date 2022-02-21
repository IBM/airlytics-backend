package com.ibm.weather.airlytics.jobs.eventspatch;
import com.ibm.weather.airlytics.common.athena.AbstractPartitionDayQueryBuilder;
import com.ibm.weather.airlytics.jobs.eventspatch.db.EventNameLikeQueryBuilder;
import com.ibm.weather.airlytics.jobs.eventspatch.db.EventNameSchemaVersionQueryBuilder;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;

public class QueryBuilderTest {

    @Test
    public void testPartionDayQB() throws Exception {
        AbstractPartitionDayQueryBuilder qb = new AbstractPartitionDayQueryBuilder("test_db", "test_table", 101, LocalDate.parse("2021-12-19")) {
            @Override
            public String build() {
                return "SELECT *" + getFromPart() + getWherePart();
            }
        };
        assertEquals(
                "SELECT * FROM test_db.test_table WHERE partition = 101 AND day = '2021-12-19'",
                qb.build());
    }

    @Test
    public void testEventNameSchemaVersionQB() throws Exception {
        EventNameSchemaVersionQueryBuilder qb =
                new EventNameSchemaVersionQueryBuilder(
                        "test_db",
                        "test_table",
                        101,
                        LocalDate.parse("2021-12-19"),
                        "test-event",
                        "1.1",
                        "errormessage LIKE '%keyword\":\"additionalProperties%'");
        assertEquals(
                "SELECT event FROM test_db.test_table WHERE partition = 101 AND day = '2021-12-19' AND name = 'test-event' AND schemaversion = '1.1' AND errormessage LIKE '%keyword\":\"additionalProperties%'",
                qb.build());
    }

    @Test
    public void testEventNameLikeQB() throws Exception {
        EventNameLikeQueryBuilder qb =
                new EventNameLikeQueryBuilder(
                        "test_db",
                        "test_table",
                        101,
                        LocalDate.parse("2021-12-19"),
                        "test-%");
        assertEquals(
                "SELECT event FROM test_db.test_table WHERE partition = 101 AND day = '2021-12-19' AND name LIKE 'test-%'",
                qb.build());
    }
}
