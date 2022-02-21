package com.ibm.weather.airlytics.dataimport.services;

import com.ibm.weather.airlytics.dataimport.dto.S3UriData;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class S3IntegrationTest {

    //@Test
    public void testS3() throws Exception {
        String header = readFirstLine("airlytics-test-data", "upsert/2_2020-10-07.csv");
        parseCsvHeader(header);
    }

    //@Test
    public void testS3Gzip() throws Exception {
        String header = readFirstLine("airlytics-test-data", "upsert/test_user_features_202009171213.csv.gz");
        parseCsvHeader(header);
    }

    //@Test
    public void testLargeS3Gzip() throws Exception {
        String header = readFirstLine("airlytics-test-data", "upsert/user_features_1M_202010251552.csv.gz");
        parseCsvHeader(header);
    }

    //@Test
    public void testCopy() throws Exception {
        String sourceBucket = "airlocktest";
        String sourceFilePath = "test1/2_2020-10-07.csv";

        String targetBucket = "airlytics-test-data";
        String targetFolder = "upsert";

        S3Client sourceS3 = buildS3Client(sourceBucket);
        HeadObjectResponse head = sourceS3.headObject(builder -> builder.bucket(sourceBucket).key(sourceFilePath));

        boolean isGzip =
                sourceFilePath.endsWith(".gz") ||
                        sourceFilePath.endsWith(".gzip") ||
                        "application/x-gzip".equalsIgnoreCase(head.contentType()) ||
                        "gzip".equalsIgnoreCase(head.contentEncoding());

        S3Client targetS3 = buildS3Client(targetBucket);

        String targetFileName = sourceFilePath.substring(sourceFilePath.lastIndexOf('/') + 1);

        if(isGzip) {
            targetS3.copyObject(builder -> builder
                    .copySource(sourceBucket + '/' + sourceFilePath)
                    .destinationBucket(targetBucket)
                    .destinationKey(targetFolder + '/' + targetFileName)
                    .contentType(head.contentType())
                    .metadata(head.metadata())
                    .metadataDirective("REPLACE")
                    .contentEncoding("gzip"));
        } else {
            targetS3.copyObject(builder -> builder
                    .copySource(sourceBucket + '/' + sourceFilePath)
                    .destinationBucket(targetBucket)
                    .destinationKey(targetFolder + '/' + targetFileName)
                    .contentType(head.contentType())
                    .metadata(head.metadata())
                    .metadataDirective("REPLACE"));
        }
    }

    @Test
    public void testFixEncoding() throws Exception {
        String sourceBucket = "aicore-segmenter";
        String sourceFilePath = "airlytics-upload/oren-test/test_user_features_202009171213.csv.gz";
        S3Client sourceS3 = buildS3Client(sourceBucket);
        HeadObjectResponse head = sourceS3.headObject(builder -> builder.bucket(sourceBucket).key(sourceFilePath));

        if(!"gzip".equalsIgnoreCase(head.contentEncoding())) {
            sourceS3.copyObject(builder -> builder
                    .copySource(sourceBucket + '/' + sourceFilePath)
                    .destinationBucket(sourceBucket)
                    .destinationKey(sourceFilePath)
                    .contentType(head.contentType())
                    .metadata(head.metadata())
                    .metadataDirective("REPLACE")
                    .contentEncoding("gzip")
                    .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL));
            System.out.println("Object copied");
        }

    }

    private S3Client buildS3Client(String bucketName) throws DataImportServiceException {

        try {
            String location = getBucketLocation(bucketName);
            Region region = Region.of(location);

            // job definition - set region
            return S3Client.builder().region(region).build();
        } catch (S3Exception s3e) {
            throw new DataImportServiceException("Error connecting to S3 URI " + bucketName, s3e);
        }
    }

    private String getBucketLocation(String bucketName) {
        S3Client s3 = S3Client.builder().region(Region.EU_WEST_1).build();
        String location = null;

        try {
            HeadBucketResponse resp = s3.headBucket(builder -> builder.bucket(bucketName));
            List<String> headerValues = resp.sdkHttpResponse().headers().get("x-amz-bucket-region");

            if(CollectionUtils.isNotEmpty(headerValues)) {
                location = headerValues.get(0);
            }
        } catch (S3Exception e) {
            if (e.statusCode() == 301) {
                List<String> errorHeaderValues = e.awsErrorDetails().sdkHttpResponse().headers().get("x-amz-bucket-region");

                if(CollectionUtils.isNotEmpty(errorHeaderValues)) {
                    location = errorHeaderValues.get(0);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (StringUtils.isBlank(location)) {
            try {
                location = s3.getBucketLocation(builder -> builder.bucket(bucketName)).locationConstraintAsString();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (StringUtils.isBlank(location) || "none".equalsIgnoreCase(location)) {
            location = "us-east-1";// default region
        }
        return location;
    }

    private String readFirstLine(String bucketName, String filePath) throws Exception {
        S3Client s3 = buildS3Client(bucketName);
        HeadObjectResponse head = s3.headObject(builder -> builder.bucket(bucketName).key(filePath));

        boolean isGzip =
                filePath.endsWith(".gz") ||
                        filePath.endsWith(".gzip") ||
                        "application/x-gzip".equalsIgnoreCase(head.contentType()) ||
                        "gzip".equalsIgnoreCase(head.contentEncoding());

        ResponseInputStream<GetObjectResponse> s3objectResponse =
        s3.getObject(builder -> builder
                        .bucket(bucketName)
                        .key(filePath)
                        .range("bytes=0-9999"));

        if(isGzip) {
            try (   InputStream fileStream = new BufferedInputStream(s3objectResponse);
                    GZIPInputStream gzipStream = new GZIPInputStream(fileStream);
                    InputStreamReader decoder = new InputStreamReader(gzipStream, "UTF-8");
                    ) {
                char[] buf = new char[10000];
                decoder.read(buf, 0, 10000);
                String line = new String(buf);
                line = line.substring(0, line.indexOf('\n'));
                assertThat(line).isNotNull();
                System.out.println(line);

                if(!"gzip".equalsIgnoreCase(head.contentEncoding())) {
                    s3.copyObject(builder -> builder
                            .copySource(bucketName + '/' + filePath)
                            .destinationBucket(bucketName)
                            .destinationKey(filePath)
                            .contentType(head.contentType())
                            .metadata(head.metadata())
                            .metadataDirective("REPLACE")
                            .contentEncoding("gzip"));
                }
                return line;
            }
        } else {
            try(BufferedReader reader = new BufferedReader(new InputStreamReader(s3objectResponse))) {
                String line = reader.readLine();
                assertThat(line).isNotNull();
                System.out.println(line);
                return line;
            }
        }
    }

    private void parseCsvHeader(String header) throws IOException, CsvException {
        CSVParser parser = new CSVParserBuilder()
                .withSeparator(',')
                .build();

        try (CSVReader csvReader = new CSVReaderBuilder(new StringReader(header))
                .withSkipLines(0)
                .withCSVParser(parser)
                .build();) {
            List<String[]> lines = csvReader.readAll();
            assertThat(lines).hasSize(1);
            System.out.println(Arrays.asList(lines.get(0)));
        }
    }
}
