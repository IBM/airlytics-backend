package com.ibm.weather.airlytics.braze.service;

import com.ibm.weather.airlytics.braze.db.BrazeExportDao;
import com.ibm.weather.airlytics.braze.dto.BrazeEntity;
import com.ibm.weather.airlytics.braze.dto.InstallOrPurchase;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

@Service
public class BrazeHistoryExtractionService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrazeHistoryExtractionService.class);

    private static final int BRAZE_BATCH_SIZE = 75;

    private AtomicInteger trialCount= new AtomicInteger(0);
    private AtomicInteger subscriptionCount = new AtomicInteger(0);
    private AtomicInteger autorenewCount = new AtomicInteger(0);
    private AtomicInteger invalidPurchaseCount = new AtomicInteger(0);

    @Value("${export.users.platform:ios}")
    private String platform;

    @Value("${installs.trials.start.shard:0}")
    private int installsTrialsStart;

    @Value("${subscriptions.start.shard:0}")
    private int subscriptionsStart;

    @Value("${autorenew.start.shard:0}")
    private int autorenewStart;

    private BrazeExportDao dao;

    private BrazeHistoryUploadService brazeService;

    @Autowired
    public BrazeHistoryExtractionService(BrazeExportDao dao, BrazeHistoryUploadService brazeService) {
        this.dao = dao;
        this.brazeService = brazeService;
    }

    @Scheduled(initialDelay = 10_000, fixedDelay = Long.MAX_VALUE)
    public void exportHistoryToBraze() {
        exportInstallsAndTrials();
        exportSubscriptions();
        exportPremiumAndAutorenew();

        if(this.brazeService.isError()) {
            LOGGER.error("Export has failed");
        } else {
            LOGGER.info(
                    "Export completed, submitted {} install dates and trials, {} subscriptions, {} statuses. Found {} invalid purchases.",
                    this.trialCount.get(),
                    this.subscriptionCount.get(),
                    this.autorenewCount.get(),
                    this.invalidPurchaseCount.get());
        }
    }

    private void exportInstallsAndTrials() {
        if(this.brazeService.isError()) return;

        LOGGER.info("Uploading installs and trials");

        PurchaseConsumer consumer = new PurchaseConsumer(this.brazeService, this.trialCount, this.invalidPurchaseCount);

        IntStream.range(installsTrialsStart, 1000).forEach(shard -> dao.sendInstallsAndTrials(platform, shard, consumer));
        consumer.sendBatch();
    }

    private void exportSubscriptions() {
        if(this.brazeService.isError()) return;

        LOGGER.info("Uploading subscriptions");

        PurchaseConsumer consumer = new PurchaseConsumer(this.brazeService, this.subscriptionCount, this.invalidPurchaseCount);

        IntStream.range(subscriptionsStart, 1000).forEach(shard -> dao.sendPurchases(platform, shard, consumer));
        consumer.sendBatch();
    }

    private void exportPremiumAndAutorenew() {
        if(this.brazeService.isError()) return;

        LOGGER.info("Uploading premium and autorenew");

        PremiumAutorenewConsumer consumer = new PremiumAutorenewConsumer(this.brazeService, this.autorenewCount);

        IntStream.range(autorenewStart, 1000).forEach(shard -> dao.sendPremiumAndAutorenewal(platform, shard, consumer));
        consumer.sendBatch();
    }

    private static class BatchingConsumer {

        protected BrazeHistoryUploadService brazeService;

        private AtomicInteger counter;

        public BatchingConsumer(BrazeHistoryUploadService brazeService, AtomicInteger counter) {
            this.brazeService = brazeService;
            this.counter = counter;
        }

        protected List<BrazeEntity> batch = new ArrayList<>(BRAZE_BATCH_SIZE);

        public void sendBatch() {
            if(this.brazeService.isError()) return;

            brazeService.asyncSendBatch(batch, counter);

            batch = new ArrayList<>(BRAZE_BATCH_SIZE);
        }

    }

    private static class PurchaseConsumer extends BatchingConsumer implements Consumer<InstallOrPurchase> {
        private AtomicInteger invalidPurchaseCount;

        public PurchaseConsumer(BrazeHistoryUploadService brazeService, AtomicInteger counter, AtomicInteger invalidPurchaseCount) {
            super(brazeService, counter);
            this.invalidPurchaseCount = invalidPurchaseCount;
        }

        @Override
        public void accept(InstallOrPurchase purchase) {
            if(this.brazeService.isError()) return;

            if(purchase.getInstallDate() == null &&
                    (purchase.getProduct() == null ||
                            purchase.getStartDate() == null ||
                            (purchase.getEndDate() == null && purchase.getTrialLengthDays() == 0) ) ) {
                invalidPurchaseCount.incrementAndGet();
                return;
            }

            BrazeEntity user = new BrazeEntity();
            Map<String, Object> properties = new HashMap<>();
            user.setProperties(properties);

            user.setKind(BrazeEntity.Kind.USER);
            user.setExternal_id(purchase.getUserId());

            if(purchase.getInstallDate() != null) {
                properties.put("installDate", purchase.getInstallDate().truncatedTo(ChronoUnit.DAYS).toString());
            }

            if(purchase.isTrial() && purchase.getProduct() != null && purchase.getStartDate() != null) {
                properties.put("meteredTrialId", purchase.getProduct());
                properties.put("meteredTrialStartDate", purchase.getStartDate().truncatedTo(ChronoUnit.DAYS).toString());
                properties.put("meteredTrialLengthInDays", purchase.getTrialLengthDays());
            } else if(!purchase.isTrial() && purchase.getProduct() != null && purchase.getStartDate() != null && purchase.getEndDate() != null) {
                properties.put("premiumProductId", purchase.getProduct());
                properties.put("premiumStartDate", purchase.getStartDate().truncatedTo(ChronoUnit.DAYS).toString());
                properties.put("premiumExpirationDate", purchase.getEndDate().truncatedTo(ChronoUnit.DAYS).toString());
            }

            batch.add(user);

            if(batch.size() == BRAZE_BATCH_SIZE) {
                sendBatch();
            }
        }
    }

    private static class PremiumAutorenewConsumer extends BatchingConsumer implements Consumer<InstallOrPurchase> {

        public PremiumAutorenewConsumer(BrazeHistoryUploadService brazeService, AtomicInteger counter) {
            super(brazeService, counter);
        }

        @Override
        public void accept(InstallOrPurchase purchase) {
            if(this.brazeService.isError()) return;

            BrazeEntity user = new BrazeEntity();
            Map<String, Object> properties = new HashMap<>();
            user.setProperties(properties);

            user.setKind(BrazeEntity.Kind.USER);
            user.setExternal_id(purchase.getUserId());

            properties.put("premium", purchase.isPremium());
            properties.put("premiumAutoRenewStatus", purchase.isPremiumAutoRenewStatus());

            if(purchase.getPremiumAutoRenewChangeDate() != null) {
                properties.put("premiumAutoRenewChangeDate", purchase.getPremiumAutoRenewChangeDate().toString());
            }

            batch.add(user);

            if(batch.size() == BRAZE_BATCH_SIZE) {
                sendBatch();
            }
        }
    }
}
