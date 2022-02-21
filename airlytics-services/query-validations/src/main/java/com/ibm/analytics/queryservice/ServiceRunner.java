package com.ibm.analytics.queryservice;

import com.ibm.analytics.queryservice.aws.S3Uploader;
import com.ibm.analytics.queryservice.config.ServiceConfig;
import com.ibm.analytics.queryservice.health.HealthCheckable;
import com.ibm.analytics.queryservice.health.MonitoringServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.testng.TestNG;
import org.testng.xml.Parser;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.springframework.core.io.ResourceLoader.CLASSPATH_URL_PREFIX;

@Service
public class ServiceRunner {

    public static final String OUTPUT_DIRECTORY = "validation-output-reports";

    ServiceRunner(){
        initMonitoringServer();
    }

    @PostConstruct
    @Scheduled(cron = "0 0 01 * * ?")
    void runQueryService() throws IOException {
        TestNG testNG = new TestNG();
        Resource resource = new DefaultResourceLoader().getResource(CLASSPATH_URL_PREFIX+"testng.xml");
        testNG.setXmlSuites(new Parser(resource.getInputStream()).parseToList());
        testNG.setUseDefaultListeners(false);
        testNG.setOutputDirectory(OUTPUT_DIRECTORY);
        testNG.run();
        String secret = System.getenv("QUERY_SERVICE_AWS_SECRET_KEY");
        String region = System.getenv("QUERY_SERVICE_AWS_REGION");
        String access_key = System.getenv("QUERY_SERVICE_AWS_ACCESS_KEY");
        if (secret !=  null && !secret.isEmpty() && access_key !=  null && !access_key.isEmpty() && region !=  null && !region.isEmpty()){
            ServiceConfig config = new ServiceConfig(region, "airlytics-query-validation-reports", access_key, secret);
            S3Uploader.uploadFileToS3Bucket( new File(OUTPUT_DIRECTORY + File.separator + "index.html"), config);
            Collection<File> files = FileUtils.listFiles(new File(OUTPUT_DIRECTORY), new PrefixFileFilter("csv-report-"), null);
            if (!files.isEmpty() ) {
                S3Uploader.uploadFileToS3Bucket(files.iterator().next(), config);
            }
        }
    }

    void initMonitoringServer(){
        int monitoringPort = 8084;
        MonitoringServer monitoringServer;
        try {
            monitoringServer = new MonitoringServer() {
                @Override
                protected List<HealthCheckable> getHealthComponents() {
                    return null;
                }
            };
            monitoringServer.startServer(monitoringPort);
        } catch (Exception ignored) {

        }
    }
}
