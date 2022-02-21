package com.ibm.weather.airlytics.cohorts.integrations;

import com.ibm.weather.airlytics.cohorts.dto.*;
import com.ibm.weather.airlytics.cohorts.services.TestAirCohortsConfigService;
import com.ibm.weather.airlytics.common.airlock.AirlockException;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;

public class TestAirlockCohortsClient extends AirlockCohortsClient {

    public static final String DB_APP_NAME = "Unit Test App 1";

    public static final String PRODUCT_ID = "unittest_product_1";

    private String cohortExpName = "unittest_exp_cohort_1";

    private String cohortName = "unittest_cohort_1";

    private String condition = "premium = true";

    private List<String> joinedTables = null;

    private String expression = null;//"(sessions_30d::numeric/in_app_message_displayed_30d::numeric)";

    private String exportType = CohortExportConfig.DB_ONLY_EXPORT;

    public TestAirlockCohortsClient() { super(new TestAirCohortsConfigService(), null); }

    public List<String> getJoinedTables() {
        return joinedTables;
    }

    public void setJoinedTables(List<String> joinedTables) {
        this.joinedTables = joinedTables;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getCohortExpName() {
        return cohortExpName;
    }

    public void setCohortExpName(String cohortExpName) {
        this.cohortExpName = cohortExpName;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getExportType() {
        return exportType;
    }

    public void setExportType(String exportType) {
        this.exportType = exportType;
    }

    @Override
    public String startSessionGetToken(boolean enforce) throws AirlockException {
        return UUID.randomUUID().toString();
    }

    @Override
    public Optional<ProductCohortsConfig> fetchAirlockProductConfig(String productId) throws AirlockException {
        ProductCohortsConfig pc = new ProductCohortsConfig();
        pc.setDbApplicationName(DB_APP_NAME);
        pc.setCohorts(new LinkedList<>());

        for(int i = 1; i <= 4; i++) {
            CohortConfig cc = new CohortConfig();
            cc.setProductId(PRODUCT_ID);
            cc.setUniqueId("unittest_cohort_" + i);
            cc.setName("unittest_cohort_name_" + i);
            cc.setJoinedTables(joinedTables);
            cc.setQueryCondition(condition);
            cc.setQueryAdditionalValue(expression);
            cc.setUpdateFrequency(CohortCalculationFrequency.values()[i]);

            if(exportType != null) {
                CohortExportConfig exportConfig = new CohortExportConfig();

                if(!CohortExportConfig.DB_ONLY_EXPORT.equals(exportType)) {
                    exportConfig.setExportName("air_test_" + i);
                }
                cc.setExports(Collections.singletonMap(exportType, exportConfig));
            }

            pc.getCohorts().add(cc);
        }
        return Optional.of(pc);
    }

    @Override
    public Optional<CohortConfig> fetchAirlockCohortConfig(String cohortId) throws AirlockException {
        CohortConfig cc = new CohortConfig();
        cc.setUniqueId(cohortId);
        cc.setProductId(UUID.randomUUID().toString());
        cc.setName(cohortName);
        cc.setJoinedTables(joinedTables);
        cc.setQueryCondition(condition);
        cc.setQueryAdditionalValue(expression);

        if(exportType != null) {
            CohortExportConfig exportConfig = new CohortExportConfig();

            if(!CohortExportConfig.DB_ONLY_EXPORT.equals(exportType)) {
                exportConfig.setExportName("air_test");
            }
            cc.setExports(Collections.singletonMap(exportType, exportConfig));
        }

        return Optional.of(cc);
    }

    @Override
    public void updateJobStatus(String cohortId, BasicJobStatusReport status) throws AirlockException {
        // do nothing, for now
    }
}
