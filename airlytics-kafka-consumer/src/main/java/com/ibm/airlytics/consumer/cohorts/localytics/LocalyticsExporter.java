package com.ibm.airlytics.consumer.cohorts.localytics;

import com.google.common.util.concurrent.RateLimiter;
import com.ibm.airlytics.consumer.cohorts.AbstractCohortsExporter;
import com.ibm.airlytics.consumer.cohorts.airlock.AirlockException;
import com.ibm.airlytics.consumer.cohorts.dto.*;
import com.ibm.airlytics.utilities.SimpleRateLimiter;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import com.opencsv.CSVWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class LocalyticsExporter extends AbstractCohortsExporter {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(LocalyticsExporter.class.getName());

    public static final String LOCALYTICS_EXPORT_KEY = "Localytics";

    private final SimpleRateLimiter batchUploadRateLimiter;
    private final RateLimiter jobStatusRateLimiter;
    private final Executor monitoringJobExecutor;

    private LocalyticsClient localyticsClient;

    private String tmpFolderPath = "./tmp";

    public LocalyticsExporter(
            LocalyticsCohortsConsumerConfig featureConfig,
            SimpleRateLimiter batchUploadRateLimiter,
            RateLimiter jobStatusRateLimiter,
            Executor monitoringJobExecutor) {
        super(featureConfig);
        this.batchUploadRateLimiter = batchUploadRateLimiter;
        this.jobStatusRateLimiter = jobStatusRateLimiter;
        this.monitoringJobExecutor = monitoringJobExecutor;
        this.localyticsClient = new LocalyticsClient(featureConfig);

        try {
            Files.createDirectories(Paths.get(tmpFolderPath));
        } catch (IOException e) {
            LOGGER.error("Could not create temp directory " + tmpFolderPath, e);
            throw new RuntimeException(e);
        }
    }

    public Map<String, Integer> sendRenamesBatch(List<UserCohort> renamesList, String productId) throws LocalyticsException {
        long start = System.currentTimeMillis();

        List<UserCohort> ucList = new LinkedList<>();
        Set<String> addedIds = new HashSet<>();
        String combinedId = null;
        ListIterator<UserCohort> listIterator = renamesList.listIterator(renamesList.size());

        // reverse to write the latest value in case of duplicates
        while (listIterator.hasPrevious()) {
            UserCohort uc = listIterator.previous();
            combinedId = uc.getUserId() + uc.getCohortId();

            if(!addedIds.contains(combinedId)) {
                ucList.add(uc);
                addedIds.add(combinedId);
            }
        }

        Set<String> cohortNames = new LinkedHashSet<>();
        List<String> cohortIds = new LinkedList<>();

        for(UserCohort uc : ucList) {
            Optional<UserCohortExport> export = getUserCohortExport(uc, LOCALYTICS_EXPORT_KEY);

            if (export.isPresent()) {
                cohortNames.add(export.get().getOldFieldName());
                cohortNames.add(export.get().getExportFieldName());

                if(!cohortIds.contains(uc.getCohortId())) {
                    cohortIds.add(uc.getCohortId());
                }
            }
        }
        Map<String, List<UserCohort>> cohortsPerUser =
                ucList.stream().collect(Collectors.groupingBy(UserCohort::getUpsId));

        String fileName = getCsvFilename();
        String filePath = tmpFolderPath + "/" + fileName;
        int total = 0;

        // Renames are done for the whole cohorts, and we can have more than 1 per user in one CSV,
        // which is not the case for deltas (see below)
        try (
                Writer writer = Files.newBufferedWriter(Paths.get(filePath));

                CSVWriter csvWriter = new CSVWriter(writer,
                        CSVWriter.DEFAULT_SEPARATOR,
                        CSVWriter.DEFAULT_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                        CSVWriter.DEFAULT_LINE_END);
        ) {
            String[] headerRecord = new String[cohortNames.size() + 1];
            headerRecord[0] = "customer ID";
            int i = 1;

            for(String name : cohortNames) {
                headerRecord[i++] = name;
            }
            csvWriter.writeNext(headerRecord);

            for(String upsId : cohortsPerUser.keySet()) {
                List<UserCohort> userCohorts = cohortsPerUser.get(upsId);

                String[] record = new String[cohortNames.size() + 1];
                record[0] = upsId;

                Map<String, String> userCohortValuesPerCohortName = new HashMap<>();

                for(UserCohort uc : userCohorts) {
                    UserCohortExport export = getUserCohortExport(uc, LOCALYTICS_EXPORT_KEY).orElse(null);

                    if(!uc.isPendingDeletion()) {
                        userCohortValuesPerCohortName.put(export.getExportFieldName(), uc.getCohortValue());
                    }
                }
                i = 1;

                for(String name : cohortNames) {
                    total++;
                    record[i++] = userCohortValuesPerCohortName.get(name);
                }
                csvWriter.writeNext(record);
            }
        } catch (IOException e) {
            throw new LocalyticsException("Error creating Localytics profiles CSV for cohorts " + cohortNames, e);
        }
        Optional<String> activityId = submitToLocalytics(fileName, filePath, productId);

        Map<String, Integer> cohortCounts = calculateProgress(cohortIds, renamesList);

        monitoringJobExecutor.execute(() -> {
            monitorJobStatus(cohortIds, activityId.orElse(null), cohortCounts);
        });

        LOGGER.info("Export for renamed cohort " + cohortNames + " took " + (System.currentTimeMillis() - start) + "ms, exporting " + total + " user-cohorts");
        usersCounter.labels(LOCALYTICS_EXPORT_KEY).inc(total);
        return cohortCounts;
    }

    public Map<String, Integer> sendExportedDeltasBatch(List<UserCohort> ucList, String productId) throws LocalyticsException {
        long start = System.currentTimeMillis();
        Map<String, Integer> progress = new HashMap<>();
        Map<String, String> cohortNames = new LinkedHashMap<>();
        List<UserCohort> submittedList = new LinkedList<>();
        int total = 0;

        for(UserCohort uc : ucList) {
            Optional<UserCohortExport> export = getUserCohortExport(uc, LOCALYTICS_EXPORT_KEY);

            if (export.isPresent()) {
                cohortNames.put(export.get().getExportFieldName(), uc.getCohortId());
            }
        }

        // We cannot export all deltas in 1 file, since we want to avoid deleting users from cohorts, where their membership did not change
        for(String cohortName : cohortNames.keySet()) {

            String fileName = getCsvFilename();
            String filePath = tmpFolderPath + "/" + fileName;

            try (
                    Writer writer = Files.newBufferedWriter(Paths.get(filePath));

                    CSVWriter csvWriter = new CSVWriter(writer,
                            CSVWriter.DEFAULT_SEPARATOR,
                            CSVWriter.DEFAULT_QUOTE_CHARACTER,
                            CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                            CSVWriter.DEFAULT_LINE_END);
            ) {
                String[] headerRecord = new String[2];
                headerRecord[0] = "customer ID";
                headerRecord[1] = cohortName;
                csvWriter.writeNext(headerRecord);
                Set<String> addedIds = new HashSet<>();
                ListIterator<UserCohort> listIterator = ucList.listIterator(ucList.size());

                // reverse to write the latest value in case of duplicates
                while (listIterator.hasPrevious()) {
                    UserCohort uc = listIterator.previous();
                    Optional<UserCohortExport> export = getUserCohortExport(uc, LOCALYTICS_EXPORT_KEY);

                    if (!export.isPresent() || !cohortName.equals(export.get().getExportFieldName()) || addedIds.contains(uc.getUpsId())) {
                        continue;
                    }
                    String[] record = new String[2];
                    record[0] = uc.getUpsId();
                    record[1] = uc.isPendingDeletion() ? null : uc.getCohortValue();
                    submittedList.add(uc);
                    total++;

                    csvWriter.writeNext(record);
                    addedIds.add(uc.getUpsId());
                }
                LOGGER.debug("Added " + addedIds.size() + " entries to " + fileName);
            } catch (IOException e) {
                throw new LocalyticsException("Error creating Localytics profiles CSV for cohorts " + cohortNames, e);
            }
            Optional<String> activityId = submitToLocalytics(fileName, filePath, productId);

            List<String> cohortIds = Collections.singletonList(cohortNames.get(cohortName));
            Map<String, Integer> cohortCounts = calculateProgress(cohortIds, ucList);

            monitoringJobExecutor.execute(() -> {
                monitorJobStatus(cohortIds, activityId.orElse(null), cohortCounts);
            });
            progress.putAll(cohortCounts);
        }
        LOGGER.info("Export for cohort " + cohortNames + " took " + (System.currentTimeMillis() - start) + "ms, exporting " + total + " user-cohorts");
        usersCounter.labels(LOCALYTICS_EXPORT_KEY).inc(total);
        return progress;
    }

    public Optional<ExportJobStatusReport> getJobStatus(String activityId) throws LocalyticsException {

        if(activityId == null) {// only when export is disabled
            ExportJobStatusReport report = new ExportJobStatusReport();
            report.setExportKey(LocalyticsClient.LOCALYTICS_EXPORT);
            report.setStatus(ExportJobStatusReport.JobStatus.RUNNING);
            report.setStatusMessage("Localytics API is disabled");
            JobStatusDetails details = new JobStatusDetails(activityId);
            details.setActivityId(UUID.randomUUID().toString());
            details.setStatus(ExportJobStatusReport.JobStatus.COMPLETED);
            report.setThirdPartyStatusDetails(details);

            return Optional.of(report);
        }

        jobStatusRateLimiter.acquire();
        return localyticsClient.getJobStatus(activityId);
    }

    private static final long MONITORING_RATE = 30_000L;// 30s
    private static final long MAX_MONITORING_PERIOD = 1200_000L;// 20m

    public void monitorJobStatus(List<String> cohortIds, String activityId, Map<String, Integer> cohortCounts) {
        String cohortId = null;

        try {
            long startTime = System.currentTimeMillis();

            while(true) {

                if((System.currentTimeMillis() - startTime) > MAX_MONITORING_PERIOD) {
                    break;
                }
                Thread.sleep(MONITORING_RATE);

                try {
                    Optional<ExportJobStatusReport> optReport = getJobStatus(activityId);

                    if (optReport.isPresent()) {

                        for(int i = 0; i < cohortIds.size(); i++) {
                            cohortId = cohortIds.get(i);

                            if(optReport.get().getThirdPartyStatusDetails() != null &&
                                    optReport.get().getThirdPartyStatusDetails().getSuccessfulImports() == null &&
                                    optReport.get().getThirdPartyStatusDetails().getFailedImports() == null) {
                                int cnt = cohortCounts.get(cohortId);

                                if(optReport.get().getThirdPartyStatusDetails().getStatus() == ExportJobStatusReport.JobStatus.FAILED) {
                                    optReport.get().getThirdPartyStatusDetails().setFailedImports(cnt);
                                } else if(optReport.get().getThirdPartyStatusDetails().getStatus() == ExportJobStatusReport.JobStatus.COMPLETED) {
                                    optReport.get().getThirdPartyStatusDetails().setSuccessfulImports(cnt);
                                }
                            }
                            airlockClient.updateExportJobStatus(cohortId, optReport.get());
                        }

                        if(optReport.get().getThirdPartyStatusDetails() == null ||
                                optReport.get().getThirdPartyStatusDetails().getStatus() == ExportJobStatusReport.JobStatus.COMPLETED ||
                                optReport.get().getThirdPartyStatusDetails().getStatus() == ExportJobStatusReport.JobStatus.FAILED) {
                            break;
                        }
                    }
                } catch (LocalyticsException e) {
                    LOGGER.error("Error getting job status from Localytics, cohort " + cohortId + ", job " + activityId, e);
                    Thread.sleep(MONITORING_RATE);
                } catch (AirlockException e) {
                    LOGGER.error("Error reporting job status to Airlock, cohort " + cohortId + ", job " + activityId, e);
                    Thread.sleep(MONITORING_RATE);
                }
            }
        } catch (InterruptedException interruptedException) {
            // interrupted
        }
    }

    public void reportAirlyticsSuccess(Map<String, Integer> progress) {

        if(MapUtils.isNotEmpty(progress)) {
            progress.forEach((cohortId, cnt) -> reportAirlyticsProgress(cohortId, cnt, ExportJobStatusReport.JobStatus.COMPLETED));
        }
    }

    public void reportAirlyticsProgress(String cohortId, int cnt, ExportJobStatusReport.JobStatus detailsStatus) {

        try {
            ExportJobStatusReport report = new ExportJobStatusReport();
            report.setExportKey(LocalyticsClient.LOCALYTICS_EXPORT);
            report.setStatus(
                    detailsStatus == ExportJobStatusReport.JobStatus.FAILED ?
                            ExportJobStatusReport.JobStatus.FAILED : ExportJobStatusReport.JobStatus.RUNNING);
            report.setStatusMessage("Batch sent to Localytics");
            JobStatusDetails details = new JobStatusDetails();
            details.setStatus(detailsStatus);
            if (detailsStatus != ExportJobStatusReport.JobStatus.FAILED) {
                details.setSuccessfulImports(cnt);
            } else {
                details.setFailedImports(cnt);
            }
            report.setAirlyticsStatusDetails(details);
            airlockClient.updateExportJobStatus(cohortId, report);
        } catch (AirlockException e) {
            LOGGER.error("Error reporting Airlytics job status to Airlock, cohort " + cohortId, e);
        }
    }

    private Map<String, Integer> calculateProgress(
            List<String> cohortIds,
            List<UserCohort> ucList) {
        Map<String, Integer> result = new HashMap<>();

        for(String cohortId : cohortIds) {
            int cnt = (int)ucList.stream().filter(uc -> cohortId.equals(uc.getCohortId())).count();
            result.put(cohortId, cnt);
        }
        return result;
    }

    private String getCsvFilename() {
        return UUID.randomUUID().toString() + ".csv";
    }

    private Optional<String> submitToLocalytics(String fileName, String filePath, String productId) throws LocalyticsException {
        Optional<String> optActivityId = Optional.empty();

        try {

            if(!localyticsClient.isApiEnabled()) {
                LOGGER.warn("API integration is disabled in this instance, the exported file '"+fileName+"' remains on the local disc");
                return optActivityId;
            }

            if(!batchUploadRateLimiter.tryAcquire()) {
                LOGGER.error("Localytics upload rate limit exceeded");
                throw new LocalyticsException("Localytics upload rate limit exceeded");
            }
            optActivityId = localyticsClient.uploadProfiles(fileName, filePath, productId);

            Files.delete(Paths.get(filePath));
        } catch (LocalyticsException e) {
            throw e;
        } catch (IOException e) {
            LOGGER.error("Error deleting uploaded temp file " + filePath, e);
        }
        return optActivityId;
    }
}
