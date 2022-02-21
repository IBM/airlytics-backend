package com.ibm.airlytics.consumer.purchase.inject;

import com.amazonaws.SdkClientException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.purchase.PurchaseConsumerConfig;
import com.ibm.airlytics.consumer.purchase.PurchasesDAO;
import com.ibm.airlytics.producer.Producer;
import com.ibm.airlytics.serialize.data.S3Serializer;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;

import javax.annotation.CheckForNull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class AndroidReceiptsInjector {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(AndroidReceiptsInjector.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private Producer purchaseTopic;
    private S3Serializer s3Serializer;
    private static HashMap<String, Integer> users = new HashMap();
    protected PreparedStatement purchaseProductIdByUserIdPreparedStatement = null;


    public AndroidReceiptsInjector(Producer purchaseTopic, PurchaseConsumerConfig config, Connection connection) throws SQLException {
        this.purchaseTopic = purchaseTopic;
        this.purchaseProductIdByUserIdPreparedStatement = PurchasesDAO.getProductIdByUserId(connection);
        this.s3Serializer = new S3Serializer(config.getS3region(), config.getS3Bucket(), 3);
    }

    public List<String> processEncodedReceipts(String bucketName, String fileName) {
        List<String> books = new ArrayList<>();
        InputStream inputStream = null;
        int linesCounter = 0;
        int injectedPurchaseTokenCounter = 0;


        try {
            inputStream = s3Serializer.readFileAsInputStream(bucketName, fileName);

            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
            String line = br.readLine();
            while (line != null) {
                String[] attributes = line.split(",");
                // skip invalid rows
                if (attributes[4].length() > 20) {
                    if (users.containsKey(attributes[3])) {
                        users.put(attributes[3], users.get(attributes[3]) + 1);
                    } else {
                        users.put(attributes[3], 1);
                    }
                    String userId = attributes[3];
                    String encodedReceipt = attributes[4];
                    String productId = getProductIdByUserId(userId);
                    if (productId != null) {
                        sendEncodedReceiptToTopic(userId, encodedReceipt, productId);
                        injectedPurchaseTokenCounter++;
                    }
                    linesCounter++;
                }
                line = br.readLine();
            }
            inputStream.close();
            LOGGER.info(" read [" + linesCounter + "]");
            LOGGER.info(" Injected [" + injectedPurchaseTokenCounter + "] purchase tokens");
        } catch (IOException | SdkClientException ioe) {
            LOGGER.error(ioe.getMessage());
        } catch (SQLException sqlException) {
            LOGGER.error(sqlException.getMessage());
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

    @CheckForNull
    private String getProductIdByUserId(String userId) throws SQLException {
        ResultSet rs;
        purchaseProductIdByUserIdPreparedStatement.setString(1, userId);
        try {
            rs = purchaseProductIdByUserIdPreparedStatement.executeQuery();
        } catch (RuntimeException e) {
            // We need this mess because of Prometheus wrapping the internal exception when using time()
            if (e.getCause() != null && e.getCause() instanceof SQLException)
                throw (SQLException) e.getCause();
            else
                throw e;
        }

        if (rs.next()) {
            return rs.getString(1);
        } else {
            return null;
        }
    }

    private void sendEncodedReceiptToTopic(String userId, String encodedReceipt, String productId) {
        JsonNode node = objectMapper.valueToTree(new InjectedAndroidUserAttributeEvent(
                encodedReceipt,
                productId,
                "ios",
                userId));
        // send to purchase topic
        purchaseTopic.sendRecord(userId, userId, node, System.currentTimeMillis());
    }
}

