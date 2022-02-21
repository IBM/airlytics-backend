package com.ibm.weather.airlytics.cohorts.db;

import com.ibm.weather.airlytics.cohorts.AbstractCohortsUnitTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class UserCohortsDaoTestEmbeddedCohorts extends AbstractCohortsUnitTest {

    private int[] cohortCounts = new int[9];

    @BeforeEach
    public void setup() {
        SplittableRandom random = new SplittableRandom();
        IntStream.range(0, 9).forEach(i -> cohortCounts[i] = 0);

        for(int i = 0; i < 1000; i++) {
            int c = random.nextInt(1, 10);
            cohortCounts[c - 1]++;
            dao.addUserCohortIfNotExists(UUID.randomUUID().toString(), c, "unittest_cohort_" + c, "true");
        }
    }

    @AfterEach
    public void cleanup()  {
        dao.getJdbcTemplate().update("delete from user_cohorts where cohort_id like ?", "unittest_cohort_%");
    }

    @Test
    public void testDao() {
        IntStream.range(1, 10).forEach(i -> assertThat(dao.countAudience("unittest_cohort_" + i)).isEqualTo(cohortCounts[i - 1]));
    }

    @Test
    public void testGetColumnNames() {
        Map<String, String> names = rodao.getTableColumnNames("users", "users");
        System.out.println(names);
        assertThat(names).isNotEmpty();

        Map<String, Map<String, String>> result = new HashMap<>();
        result.put("users", names);
        ResponseEntity<Map<String, Map<String, String>>> response = ResponseEntity.ok(result);
        assertThat(response).isNotNull();
        System.out.println(response.toString());
    }
}
