package com.ibm.airlytics.consumer.purchase.inject;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.purchase.PurchaseConsumer;
import com.ibm.airlytics.consumer.purchase.PurchaseConsumerConfig;
import com.ibm.airlytics.producer.Producer;
import com.ibm.airlytics.serialize.data.S3Serializer;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import kafka.tools.ConsoleConsumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


public class IOSReceiptsInjector {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(IOSReceiptsInjector.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private Producer purchaseTopic;
    private S3Serializer s3Serializer;

    public IOSReceiptsInjector(Producer purchaseTopic, PurchaseConsumerConfig config) {
        this.purchaseTopic = purchaseTopic;
        this.s3Serializer = new S3Serializer(config.getS3region(), config.getS3Bucket(), 3);
    }


    public List<String> processEncodedReceipts(String bucketName, String fileName) {
        List<String> books = new ArrayList<>();
        InputStream inputStream = null;
        int linesCounter = 0;
        try {
            inputStream = s3Serializer.readFileAsInputStream(bucketName, fileName);

            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
            String line = br.readLine();
            while (line != null) {
                String[] attributes = line.split(",");
                // skip invalid rows
                if (attributes[4].length() > 20) {
                    sendEncodedReceiptToTopic(attributes[3], attributes[4], attributes[2]);
                    linesCounter++;
                }
                line = br.readLine();
            }
            inputStream.close();
            LOGGER.info(" Injected [" + linesCounter + "] encoded receipts");
        } catch (IOException | SdkClientException ioe) {
            LOGGER.error(ioe.getMessage());
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }


        return books;
    }


    private void sendEncodedReceiptToTopic(String userId, String encodedReceipt, String productId) {
        JsonNode node = objectMapper.valueToTree(new InjectedIOSUserAttributeEvent(
                encodedReceipt,
                productId,
                "ios",
                userId));
        // send to purchase topic
        purchaseTopic.sendRecord(userId, userId, node, System.currentTimeMillis());
    }
}

