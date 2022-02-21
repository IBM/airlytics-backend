package com.ibm.airlytics.consumer.multithreaded;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.utilities.Hashing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;

public abstract class MTConsumer extends AirlyticsConsumer {
    private static final Logger LOGGER = Logger.getLogger(AirlyticsConsumer.class.getName());

    private Hashing hashing;
    private ThreadPoolExecutor executor;
    private int threads;
    private long totalProgress = 0;
    private long lastProgress = 0;
    private Instant lastRecordProcessed = Instant.now();

    public MTConsumer(MTConsumerConfig config) {
        super(config);
        threads = config.getThreads();
        hashing = new Hashing(threads);
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threads);
    }

    @Override
    protected int processRecords(ConsumerRecords<String, JsonNode> records) {
        int recordsDone = 0;

        if (records.count() > 0) {

            // partition to threads based on the hash of the userid
            PartitionedRecordList<String, JsonNode> recordList = new PartitionedRecordList<>(threads,
                    (record) -> hashing.hashMurmur2(record.key()) % threads);

            for (ConsumerRecord<String, JsonNode> record : records)
                recordList.addRecord(record);

            Future<Integer> taskFutures[] = new Future[threads];

            for (int i = 0; i < threads; ++i) {
                final int partition = i;
                taskFutures[i] = executor.submit(() -> threadProcessRecords(recordList.getRecords(partition)));
            }

            try {
                for (int i = 0; i < threads; ++i) {
                    recordsDone += taskFutures[i].get();
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            totalProgress += recordsDone;
            onThreadsCompleteRecords();
        }

        Instant now = Instant.now();
        Duration timePassed = Duration.between(lastRecordProcessed, now);
        if (timePassed.compareTo(Duration.ofSeconds(5)) >= 0) {
            LOGGER.info("Processed " + totalProgress + " records. Current rate: " +
                    ((double) (totalProgress - lastProgress)) / timePassed.toMillis() * 1000 + " records/sec");
            lastRecordProcessed = now;
            lastProgress = totalProgress;
        }

        return recordsDone;
    }

    abstract protected Integer threadProcessRecords(Iterable<ConsumerRecord<String, JsonNode>> records) throws Exception;

    protected void onThreadsCompleteRecords() {
        commit();
    }
}
