package com.ibm.airlytics.consumer.cohorts.braze;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.braze.forwarding.BrazeApiClient;
import com.ibm.airlytics.consumer.cohorts.dto.UserCohort;
import com.ibm.airlytics.consumer.cohorts.dto.UserCohortExport;
import com.ibm.airlytics.consumer.cohorts.dto.ValueType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BrazeExporterTest {

    static final String PRODUCT_1 = "P1";

    static final String PRODUCT_2 = "P2";

    static final String COHORT_1 = "C1";

    static final String COHORT_2 = "C2";

    static final ObjectMapper mapper = new ObjectMapper();

    static BrazeExporter brazeExporter;

    static TestBrazeClient brazeApiClient;

    @BeforeClass
    public static void createExporter() throws Exception {
        brazeApiClient = new TestBrazeClient();
        Map<String, BrazeApiClient> clients = new HashMap<>();
        clients.put(PRODUCT_1.toLowerCase(), brazeApiClient);
        clients.put(PRODUCT_2.toLowerCase(), brazeApiClient);
        brazeExporter = new BrazeExporter();
        brazeExporter.setBrazeClientPerProduct(clients);
    }

    @Test
    public void testProcessBatch() throws Exception {
        brazeExporter.sendExportedDeltasBatch(generateInput());
        for(JsonNode a : brazeApiClient.getAttributes()) {
            System.out.println(mapper.writeValueAsString(a));
        }
        assertEquals(4, brazeApiClient.getAttributes().size());
    }

    private List<UserCohort> generateInput() {
        List<UserCohort> input = new LinkedList<>();

        UserCohortExport p1Export = new UserCohortExport();
        p1Export.setExportType(BrazeExporter.BRAZE_EXPORT_KEY);
        p1Export.setExportFieldName("p1_cohort");

        UserCohortExport p1RenameExport = new UserCohortExport();
        p1RenameExport.setExportType(BrazeExporter.BRAZE_EXPORT_KEY);
        p1RenameExport.setExportFieldName("p1_renamed");
        p1RenameExport.setOldFieldName("p1_cohort");

        UserCohortExport p2Export = new UserCohortExport();
        p2Export.setExportType(BrazeExporter.BRAZE_EXPORT_KEY);
        p2Export.setExportFieldName("p2_cohort");

        input.add(getUserCohort(PRODUCT_1, COHORT_1, "U1", p1Export, "3.14159", ValueType.FLOAT));
        input.add(getUserCohort(PRODUCT_2, COHORT_2, "U1", p2Export, "{OR,GE}", ValueType.ARRAY));
        input.add(getUserCohort(PRODUCT_1, COHORT_1, "U2", p1Export, null, null));// deletion
        input.add(getUserCohort(PRODUCT_1, COHORT_1, "U3", p1RenameExport, "false", ValueType.BOOL));

        return input;
    }

    private UserCohort getUserCohort(String product, String cohort, String user, UserCohortExport p1Export, String value, ValueType type) {
        UserCohort uc = new UserCohort();
        uc.setProductId(product);
        uc.setCohortId(cohort);
        uc.setCohortName(cohort);
        uc.setUserId(user);
        uc.setEnabledExports(Arrays.asList(p1Export));
        uc.setPendingDeletion(value == null);

        if(value != null) {
            uc.setCohortValue(value);
            uc.setValueType(type);
        }
        return uc;
    }
}
