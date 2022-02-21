package com.ibm.airlytics.utilities.validationService.queryservice;

import com.ibm.airlytics.utilities.validationService.queryservice.aws.S3Uploader;
import com.ibm.airlytics.utilities.validationService.queryservice.config.ServiceConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;

import org.testng.TestNG;
import org.testng.xml.Parser;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

public class QueryValidatorRunner {


    public static void main(String[] args) {
        try {
            new QueryValidatorRunner().runQueryService();
        } catch (IOException e) {

        }
        return;
    }

    void runQueryService() throws IOException {
        TestNG testNG = new TestNG();
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("queryValidator/testng.xml");

        // the stream holding the file content
        if (inputStream == null) {
            throw new IllegalArgumentException("file not found! " + "queryValidator/testng.xml");
        }
        testNG.setXmlSuites(new Parser(inputStream).parseToList());
        testNG.setUseDefaultListeners(false);
        testNG.setOutputDirectory(ServiceConfig.OUTPUT_DIRECTORY);
        testNG.run();
        String secret = System.getenv("QUERY_SERVICE_AWS_SECRET_KEY");
        String region = System.getenv("QUERY_SERVICE_AWS_REGION");
        String access_key = System.getenv("QUERY_SERVICE_AWS_ACCESS_KEY");
        if (secret !=  null && !secret.isEmpty() && access_key !=  null && !access_key.isEmpty() && region !=  null && !region.isEmpty()){
            ServiceConfig config = new ServiceConfig(region, "airlytics-query-validation-reports", access_key, secret);
            S3Uploader.uploadFileToS3Bucket( new File(ServiceConfig.OUTPUT_DIRECTORY + File.separator + "index.html"), config);
            Collection<File> files = FileUtils.listFiles(new File(ServiceConfig.OUTPUT_DIRECTORY), new PrefixFileFilter("csv-report-"), null);
            if (!files.isEmpty() ) {
                S3Uploader.uploadFileToS3Bucket(files.iterator().next(), config);
            }
        }
    }
}
