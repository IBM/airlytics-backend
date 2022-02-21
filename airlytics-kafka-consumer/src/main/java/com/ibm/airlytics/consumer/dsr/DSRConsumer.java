package com.ibm.airlytics.consumer.dsr;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.dsr.db.DBConfig;
import com.ibm.airlytics.consumer.dsr.db.DBROConfig;
import com.ibm.airlytics.consumer.dsr.db.DbHandler;
import com.ibm.airlytics.consumer.dsr.retriever.DSRData;
import com.ibm.airlytics.consumer.dsr.retriever.ParquetRetriever;
import com.ibm.airlytics.consumer.dsr.writer.DSRResponseWriter;
import com.ibm.airlytics.consumer.dsr.writer.DeleteWriter;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.DSR_REQUEST_RESULT_NOT_FOUND;
import static com.ibm.airlytics.consumer.dsr.DSRRequest.REQUEST_TYPE_DELETE;
import static com.ibm.airlytics.consumer.dsr.DSRRequest.REQUEST_TYPE_PORTABILITY;

public class DSRConsumer extends AirlyticsConsumer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(com.ibm.airlytics.consumer.dsr.DSRConsumer.class.getName());

    static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_dsr_requests_processed_total")
            .help("Total DSR requests processed by the dsr consumer.")
            .labelNames("event_name", "result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    public static final Counter dsrRequestsProcessedCounter = Counter.build()
            .name("airlytics_dsr_requests_portability_written_total")
            .help("Total DSR portability requests processed (data retrieved/deleted) by the dsr consumer.")
            .labelNames(AirlyticsConsumerConstants.DSR_REQUEST_TYPE,AirlyticsConsumerConstants.DSR_REQUEST_RESULT, AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    private DSRConsumerConfig config;
    private DSRConsumerConfig newConfig = null;
    private ParquetRetriever parquetRetriever;
    private DSRResponseWriter responseWriter;
    private DeleteWriter deleteWriter;

    private long totalProgress = 0;
    private long lastProgress = 0;
    private Instant lastRecordProcessed = Instant.now();

    public DSRConsumer(DSRConsumerConfig config) {
        super(config);
        this.config = config;
        try {
            DbHandler dbHandlerRO = new DbHandler(new DBROConfig());
            DbHandler dbHandlerRW = new DbHandler(new DBConfig());
            this.parquetRetriever = new ParquetRetriever(dbHandlerRO);
            this.responseWriter = new DSRResponseWriter(dbHandlerRW);
            this.deleteWriter = new DeleteWriter(dbHandlerRW);
        } catch (IOException | SQLException | ClassNotFoundException e) {
            LOGGER.error("error in initializing DSRConsumer:" + e.getMessage());
            e.printStackTrace();
            stop();
        }


        //on exception: stop();
    }

    @Override
    protected int processRecords(ConsumerRecords<String, JsonNode> records) {
        if (records.isEmpty())
            return 0;

        LOGGER.info("DSR consumer - process records:" + records.count());

        updateToNewConfigIfExists();

        int recordsProcessed = 0;
        List<DSRRequest> portabilityRequests = new ArrayList<>();
        List<DSRRequest> deleteRequests = new ArrayList<>();

        for (ConsumerRecord<String, JsonNode> record : records) {
            String eventName = "";
            String result = "success";

            try {
                LOGGER.info("processing record:" + record.toString());
                DSRRequest request = new DSRRequest(record);
                if (request.getUpsId()==null && request.getAirlyticsId()==null) {
                    LOGGER.error("record with empty UPS id and airlytics ID.:"+record.toString());
                } else if (request.getRequestType()==null) {
                    LOGGER.error("record with empty rType value:"+record);
                }
                else if (request.getRequestType().equals(REQUEST_TYPE_PORTABILITY)) {
                    // Do your thing Elik :)
                    portabilityRequests.add(request);
                } else if (request.getRequestType().equals(REQUEST_TYPE_DELETE)) {
                    deleteRequests.add(request);
                }

            } catch (NullPointerException e) {
                e.printStackTrace();
                errorsProducer.sendRecord(record.key(), record.value(), record.timestamp());
                result = "error";
            } finally {
                recordsProcessedCounter.labels(eventName, result,
                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                ++recordsProcessed;
            }
        }
        LOGGER.info("found " + portabilityRequests.size() + " portability requests");
        LOGGER.info("found " + deleteRequests.size() + " delete requests");
        try {
            List<DSRData> results = parquetRetriever.getUserData(portabilityRequests);
            long notFound = portabilityRequests.size() - results.size();
            if (notFound > 0) {
                LOGGER.info("did not find prod "+notFound+" portability requests");
                dsrRequestsProcessedCounter.labels(AirlyticsConsumerConstants.DSR_REQUEST_TYPE_PORTABILITY, DSR_REQUEST_RESULT_NOT_FOUND,AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
            }

            if (results.size() > 0) {
                LOGGER.info("writing "+results.size()+" portability results with "+ results.get(0).getUsersData().size()+" responses");
            }
            responseWriter.writeDRSResponses(results);

            //retrieve and find dev users
            List<DSRData> devResults = parquetRetriever.getDevUserData(portabilityRequests);
            long devNotFound = portabilityRequests.size() - devResults.size();
            if (devNotFound > 0) {
                LOGGER.info("did not find dev "+notFound+" portability requests");
            }

            if (devResults.size() > 0) {
                LOGGER.info("writing "+devResults.size()+" dev portability results with "+ devResults.get(0).getUsersData().size()+" responses");
            }
            responseWriter.writeDRSResponses(devResults);

        } catch (SQLException | ClassNotFoundException | IOException e) {
            LOGGER.error("error in retrieving data:" + e.getMessage());
            e.printStackTrace();
        }
        
        try {
            //mark for delete in parquet files
            deleteWriter.writeDeleteFromParquetRequests(deleteRequests, false);
            //now, dev users
            deleteWriter.writeDeleteFromParquetRequests(deleteRequests, true);

            //actually delete from DB
            Map<String, List<DSRRequest>> deleteRequestsByProduct = getRequestsByPlatform(deleteRequests);
            for (Map.Entry<String, List<DSRRequest>> entry : deleteRequestsByProduct.entrySet()) {
                String product = entry.getKey();
                List<DSRRequest> prodRequests = entry.getValue();
                deleteWriter.performDeleteRequestsFromDB(prodRequests, product);
            }

            //write delete notification for each request
            //disabled for now, uncomment when DSR side is ready
            deleteWriter.writeDeleteNotifications(deleteRequests);
        } catch (SQLException | ClassNotFoundException throwables) {
            LOGGER.error("error in performing delete requests:"+throwables.getMessage());
            throwables.printStackTrace();
        }

        commit();

        totalProgress += recordsProcessed;

        Instant now = Instant.now();
        Duration timePassed = Duration.between(lastRecordProcessed, now);
        if (timePassed.compareTo(Duration.ofSeconds(5)) >= 0) {
            LOGGER.info("Processed " + totalProgress + " records. Current rate: " +
                    ((double) (totalProgress - lastProgress)) / timePassed.toMillis() * 1000 + " records/sec");
            lastRecordProcessed = now;
            lastProgress = totalProgress;
        }

        return recordsProcessed;
    }

    private Map<String, List<DSRRequest>> getRequestsByPlatform(List<DSRRequest> requests) {
        Map<String, List<DSRRequest>> map = new HashMap<>();
        for (DSRRequest request : requests) {
            String product = request.getProduct();

            if (!map.containsKey(product)) {
                map.put(product, new ArrayList<>());
            }
            List<DSRRequest> currRequests = map.get(product);
            currRequests.add(request);
        }
        return map;
    }

    @Override
    protected void rollback() {
        super.rollback();
    }

    @Override
    protected void commit() {
        //try {
        // Do whatever flushes are necessary before committing to kafka
        super.commit(); // Commit to Kafka
        /*} catch (Exception e) {
            e.printStackTrace();
            // Don't bother to rollback kafka. Just exit the process (close the consumer on the way) and let Kube restart this service.
            // rollback();
            stop();
        }*/
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    private String healthMessage = "";

    @Override
    public boolean isHealthy() {
        if (!super.isHealthy())
            return false;

        /*try {
            if (!conn.isValid(3)) {
                healthMessage = "DB Connection Invalid";
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            healthMessage = e.getMessage();
            return false;
        }*/

        healthMessage = "Healthy!";
        return true;
    }

    @Override
    public String getHealthMessage() {
        return healthMessage;
    }

    @Override
    public synchronized void newConfigurationAvailable() {
        DSRConsumerConfig config = new DSRConsumerConfig();
        try {
            config.initWithAirlock();
            newConfig = config;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Note that this will NOT update Kafka URLs etc. If those change the consumer must be restarted.
    private synchronized void updateToNewConfigIfExists() {
        if (newConfig != null) {
            config = newConfig;
            newConfig = null;
        }
    }
}
