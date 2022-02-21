package com.ibm.weather.airlytics.dataimport.db;

import com.ibm.weather.airlytics.dataimport.AbstractDataImportUnitTest;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class UserFeaturesDaoTest extends AbstractDataImportUnitTest {

    @Test
    public void testGetColumnNames() {
        Map<String, String> names = dao.getTableColumns("users","user_features_test");
        System.out.println(names);
        assertThat(names).isNotEmpty();

        ResponseEntity<Map<String, String>> response = ResponseEntity.ok(names);
        assertThat(response).isNotNull();
        System.out.println(response.toString());
    }
}
