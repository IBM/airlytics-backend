package com.ibm.weather.airlytics.cohorts.services;

import com.ibm.weather.airlytics.cohorts.db.UserCohortsReadOnlyDao;
import com.ibm.weather.airlytics.cohorts.dto.*;
import com.ibm.weather.airlytics.cohorts.integrations.AirlockCohortsClient;
import com.ibm.weather.airlytics.cohorts.db.UserCohortsDao;
import com.ibm.weather.airlytics.common.airlock.AirlockException;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

@Service
public class CohortCalculationService {

    private static final Logger logger = LoggerFactory.getLogger(CohortCalculationService.class);

    private static final Gauge usersGauge =
            Gauge.build()
                    .name("airlytics_cohorts_users_gauge")
                    .help("Gauge for changes in user numbers per cohort")
                    .labelNames("cohort","env","product")
                    .register();

    private static final Summary cohortCalculationTimes =
            Summary.build()
                    .name("airlytics_cohorts_calc_duration_summary")
                    .help("Time for cohort calculation.")
                    .quantile(0.95, 0.01)
                    .labelNames("cohort","env","product")
                    .register();

    @Value("${airlock.airlytics.env:}")
    private String airlyticsEnv;

    @Value("${airlock.airlytics.product:}")
    private String airlyticsProduct;

    private AirlockCohortsClient airlockClient;

    private UserCohortsDao userCohortsDao;

    private UserCohortsReadOnlyDao userCohortsReadOnlyDao;

    private AirCohortsConfigService featureConfigService;

    private Map<String, String> productAppNamesCache = new HashMap<>();

    private String dbSchema;

    @Autowired
    public CohortCalculationService(
            AirlockCohortsClient airlockClient,
            UserCohortsDao userCohortsDao,
            UserCohortsReadOnlyDao userCohortsReadOnlyDao,
            AirCohortsConfigService featureConfigService,
            @Value("${spring.datasource.jdbc-url}") String dbUrl) {
        this.airlockClient = airlockClient;
        this.userCohortsDao = userCohortsDao;
        this.userCohortsReadOnlyDao = userCohortsReadOnlyDao;
        this.featureConfigService = featureConfigService;
        this.dbSchema = dbUrl.substring(dbUrl.lastIndexOf('=') + 1);
    }

    public BasicJobStatusReport validateCondition(String productId, List<String> joinedTables, String condition, String expression, String exportType, boolean limit) throws Exception {
        long start = System.currentTimeMillis();

        AirCohortsAirlockConfig featureConfig = getAirlockConfig();
        processJoinedTables(featureConfig, joinedTables);
        BasicJobStatusReport report = new BasicJobStatusReport(
                ExportJobStatusReport.JobStatus.COMPLETED,
                userCohortsReadOnlyDao.buildCohortValidationQuery(
                        joinedTables,
                        featureConfig.getJoinColumns(),
                        condition != null ? condition.replaceAll("\\s+", " ") : condition,
                        expression != null ? expression.replaceAll("\\s+", " ") : expression,
                        CohortExportConfig.UPS_EXPORT.equals(exportType)),
                userCohortsReadOnlyDao.validateCondition(
                        joinedTables,
                        featureConfig.getJoinColumns(),
                        condition,
                        expression,
                        CohortExportConfig.UPS_EXPORT.equals(exportType),
                        limit),
                null);
        logger.info("Validation for query '{}' took {}ms, found {} users",
                report.getStatusMessage(),
                (System.currentTimeMillis() - start),
                report.getUsersNumber());
        return report;
    }

    @Transactional
    public CohortConfig runCohortCalculation(String cohortId) throws CohortServiceException, AirlockException {
        CohortConfig cc = fetchCohortConfig(cohortId);
        runCohortCalculation(cc);
        return cc;
    }

    // since it's a DB-heavy operation, allow one call at a time
    @Transactional
    public synchronized void runCohortCalculation(CohortConfig cohortConfig) throws AirlockException, CohortServiceException {

        if(cohortConfig == null) {
            throw new IllegalArgumentException("Exporting a cohort requires CohortConfig parameter");
        }

        if(!isValidCohortConfig(cohortConfig)) {
            throw new CohortServiceException(String.format("Config for cohort %s is disabled in Airlock", cohortConfig.getUniqueId()));
        }

        if(StringUtils.isBlank(cohortConfig.getQueryCondition())) {
            throw new CohortServiceException(String.format("Config for cohort %s is invalid", cohortConfig.getUniqueId()));
        }
        Summary.Child latency = cohortCalculationTimes.labels(cohortConfig.getName(), airlyticsEnv, airlyticsProduct);
        Summary.Timer timer = latency.startTimer();
        try {
            long start = System.currentTimeMillis();
            AirCohortsAirlockConfig featureConfig = getAirlockConfig();
            List<String> joinedTables = cohortConfig.getJoinedTables() == null ? null : new ArrayList<>(cohortConfig.getJoinedTables());
            processJoinedTables(featureConfig, joinedTables);
            userCohortsDao.runCohortCalculation(
                    cohortConfig,
                    joinedTables,
                    featureConfig.getJoinColumns(),
                    cohortConfig.getExports().containsKey(CohortExportConfig.UPS_EXPORT));
            logger.info("Calculation for cohort {}:{} took {}ms, found {} users",
                    cohortConfig.getName(),
                    cohortConfig.getUniqueId(),
                    (System.currentTimeMillis() - start),
                    cohortConfig.getUsersNumber());

            usersGauge.labels(cohortConfig.getName(), airlyticsEnv, airlyticsProduct).set(cohortConfig.getUsersNumber().doubleValue());
        } finally {
            timer.observeDuration();
        }
    }

    @Transactional
    public CohortConfig markCohortForDeletion(String cohortId) throws CohortServiceException {
        CohortConfig cc = fetchCohortConfig(cohortId);
        userCohortsDao.markCohortForDeletion(cohortId);
        return cc;
    }

    @Transactional
    public void deleteUserCohort(String cohortId) throws CohortServiceException {
        userCohortsDao.deleteAllCohort(cohortId);
    }

    public CohortConfig fetchCohortConfig(String cohortId) throws CohortServiceException {

        try {
            Optional<CohortConfig> optConfig = airlockClient.fetchAirlockCohortConfig(cohortId);

            if (!optConfig.isPresent()) {
                throw new CohortServiceException(String.format("Config for cohort %s not found in Airlock", cohortId));
            }
            return optConfig.get();

        } catch (AirlockException e) {
            throw new CohortServiceException("Error obtaining cohort config: " + cohortId, e);
        }
    }

    public ProductCohortsConfig fetchProductCohortsConfig(String productId) throws CohortServiceException {

        try {
            Optional<ProductCohortsConfig> optProductConfig = airlockClient.fetchAirlockProductConfig(productId);

            if(!optProductConfig.isPresent()) {
                throw new CohortServiceException(String.format("Config for product %s not found in Airlock", productId));
            }
            return optProductConfig.get();

        } catch (AirlockException e) {
            throw new CohortServiceException("Error obtaining product config: " + productId, e);
        }
    }

    public Map<String, Map<String, String>> getTableColumnNames(String table) throws AirlockException {
        final Map<String, Map<String, String>> result = new LinkedHashMap<>();

        if("all".equalsIgnoreCase(table) || StringUtils.isBlank(table)) {
            AirCohortsAirlockConfig featureConfig = getAirlockConfig();
            List<String> tables = new LinkedList<>();
            tables.add("users");

            if(CollectionUtils.isNotEmpty(featureConfig.getAdditionalTables())) {
                tables.addAll(featureConfig.getAdditionalTables());
            }
            tables.forEach(t -> {
                result.put(t, userCohortsReadOnlyDao.getTableColumnNames(dbSchema, t));
            });
        } else {
            Map<String, String> columns = userCohortsReadOnlyDao.getTableColumnNames(dbSchema, table);
            result.put(table, columns);
        }
        return result;
    }

    public boolean isValidCohortConfig(CohortConfig cc) {
        return StringUtils.isNotBlank(
                cc.getQueryCondition()) &&
                MapUtils.isNotEmpty(
                        cc.getExports()) &&
                cc.getExports()
                        .entrySet()
                        .stream()
                        .anyMatch(e ->
                                e.getKey().equalsIgnoreCase(CohortExportConfig.DB_ONLY_EXPORT) ||
                                        StringUtils.isNotBlank(e.getValue().getExportName()));
    }

    private AirCohortsAirlockConfig getAirlockConfig() throws AirlockException {
        Optional<AirCohortsAirlockConfig> optConfig = featureConfigService.getCurrentConfig();

        if(!optConfig.isPresent()) {
            throw new AirlockException("Error reading Airlock feature config");
        }
        return optConfig.get();
    }

    private void processJoinedTables(AirCohortsAirlockConfig featureConfig, List<String> joinedTables) throws AirlockException {

        if(CollectionUtils.isNotEmpty(joinedTables)) {

            joinedTables.remove("users");
            joinedTables.remove("users_pi");
        }

        if(CollectionUtils.isNotEmpty(joinedTables)) {

            if(CollectionUtils.isEmpty(featureConfig.getAdditionalTables())) {
                throw new AirlockException("Joined tables are not allowed in Airlock config");
            }

            if(!featureConfig.getAdditionalTables().containsAll(joinedTables)) {
                throw new AirlockException("Some of the joined tables are not allowed in Airlock config");
            }
        }
    }

    void setAirlyticsVarsForTest(String env, String product) {
        this.airlyticsEnv = env;
        this.airlyticsProduct = product;
    }
}
