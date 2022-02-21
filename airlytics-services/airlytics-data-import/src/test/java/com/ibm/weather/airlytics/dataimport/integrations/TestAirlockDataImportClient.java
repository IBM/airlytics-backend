package com.ibm.weather.airlytics.dataimport.integrations;

import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.dataimport.dto.JobStatusReport;

import java.util.*;

public class TestAirlockDataImportClient extends AirlockDataImportClient {

    private Map<String, List<JobStatusReport>> reports;

    public TestAirlockDataImportClient() {
        super(null, null);
        reports = Collections.synchronizedMap(new HashMap<>());
    }

    @Override
    public void updateJobStatus(String productId, String jobId, JobStatusReport status) throws AirlockException {
        List<JobStatusReport> jobReports = reports.get(jobId);

        if(jobReports == null) {
            jobReports = new LinkedList<>();
            reports.put(jobId, jobReports);
        }
        jobReports.add(status);
    }

    @Override
    public void asyncUpdateJobStatus(String productId, String jobId, JobStatusReport status) {

        try {
            updateJobStatus(productId, jobId, status);
        } catch (AirlockException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void init() throws AirlockException {
    }
}
