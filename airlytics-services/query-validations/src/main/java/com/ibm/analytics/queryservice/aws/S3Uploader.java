package com.ibm.analytics.queryservice.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.ibm.analytics.queryservice.config.ServiceConfig;
import com.ibm.analytics.queryservice.ServiceLogger;
import software.amazon.awssdk.core.exception.SdkClientException;

import java.io.File;

public class S3Uploader {

    public static void uploadFileToS3Bucket(File fileToUpload, ServiceConfig config){

        AWSCredentials credentials = new AWSCredentials() {
            @Override
            public String getAWSAccessKeyId() {
                return config.getAccessKey();
            }

            @Override
            public String getAWSSecretKey() {
                return config.getSecret();
//                return System.getenv("AWS_QUERY_SERVICE_SECRET_KEY");
            }
        };
        try {
            //This code expects that you have AWS credentials set up per:
            // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().
                    withRegion(config.getRegion()).
                    withCredentials(new AWSStaticCredentialsProvider(credentials)).
                    build();

            // Upload a text string as a new object.
            //s3Client.putObject(bucketName, stringObjKeyName, "Uploaded String Object");

            // Upload a file as a new object with ContentType and title specified.
            PutObjectRequest request = new PutObjectRequest(config.getBucketName(), fileToUpload.getName(), fileToUpload);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("plain/text");
//            metadata.addUserMetadata("title", "reportTest");
            request.setMetadata(metadata);
            s3Client.putObject(request);
        } catch (AmazonServiceException e) {
            ServiceLogger.getLogger(S3Uploader.class.getName()).warn("Failed to upload report to s3: " + e.getMessage());
        } catch (SdkClientException e) {
            ServiceLogger.getLogger(S3Uploader.class.getName()).warn("Failed to upload report to s3: " + e.getMessage());
        }
    }
}
