package com.ibm.airlytics.consumer.purchase.ios;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.purchase.AirlyticsInAppProduct;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchaseEvent;
import com.ibm.airlytics.consumer.purchase.PurchaseConsumer;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class IOSAirlyticsPurchaseEventTest {
    private static final String TEST_DATE_FOLDER = "ios_consumer_test_data";


    @SuppressWarnings({"SpellCheckingInspection"})
    private static final String inAppProducts = "{\"com.product.iap.renewing.1year.ip1\":{\"priceUSDMicros\":9990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.ip1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1monthtrial.1\":{\"priceUSDMicros\":990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1monthtrial.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1month.1b\":{\"priceUSDMicros\":990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1month.1b\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.pro\":{\"priceUSDMicros\":9990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.pro\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.3months.1\":{\"priceUSDMicros\":9990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.3months.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1month.1\":{\"priceUSDMicros\":990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1month.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1month.pro\":{\"priceUSDMicros\":990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1month.pro\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.1\":{\"priceUSDMicros\":9990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.inapp.sub.1year.1\":{\"priceUSDMicros\":3990000,\"gracePeriod\":null,\"productId\":\"com.product.inapp.sub.1year.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.1c\":{\"priceUSDMicros\":39990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.1c\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.sub.renewing.1year.2\":{\"priceUSDMicros\":9990000,\"gracePeriod\":null,\"productId\":\"com.product.sub.renewing.1year.2\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.1b\":{\"priceUSDMicros\":19990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.1b\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null}}";
    private HashMap<String, AirlyticsInAppProduct> inAppAirlyticsProducts;

    private static final ObjectMapper objectMapper = new ObjectMapper();


    @Before
    public void setup() {
        try {
            inAppAirlyticsProducts = objectMapper.readValue(inAppProducts, new TypeReference<HashMap<String, AirlyticsInAppProduct>>() {
            });
        } catch (JsonProcessingException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testActiveRenewablePurchaseWithNotificationDID_RENEW() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_RENEW.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "monthly_receipt.json"), IOSubscriptionPurchase.class);
            LatestReceiptInfo latestReceiptInfo = ioSubscriptionPurchase.getLatestReceiptInfo().get(0);


            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());

            AirlyticsPurchaseEvent airlyticsPurchaseEvent = airlyticsPurchaseEvents.get(0);
            Assert.assertEquals(latestReceiptInfo.getPurchaseDateMs(), airlyticsPurchaseEvent.getEventTime());
            Assert.assertEquals(false, airlyticsPurchaseEvent.getAutoRenewStatus());
            Assert.assertEquals(inAppAirlyticsProducts.get(airlyticsPurchaseEvent.getProduct()).getPriceUSDMicros(),
                    airlyticsPurchaseEvent.getRevenueUsd());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.RENEWED.name(),
                    airlyticsPurchaseEvent.getName());

            Assert.assertEquals(latestReceiptInfo.getExpiresDateMs(),
                    airlyticsPurchaseEvent.getExpirationDate());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testNotActiveRenewablePurchaseWithAppEvent() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "monthly_receipt.json"), IOSubscriptionPurchase.class);
            LatestReceiptInfo lastReceiptInfo = ioSubscriptionPurchase.getLatestReceiptInfo().get(0);
            LatestReceiptInfo startReceiptInfo = ioSubscriptionPurchase.getLatestReceiptInfo().
                    get(ioSubscriptionPurchase.getLatestReceiptInfo().size() - 1);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(10, airlyticsPurchaseEvents.size());

            AirlyticsPurchaseEvent start = airlyticsPurchaseEvents.get(0);
            validatePurchaseFields(start,
                    startReceiptInfo);

            validateBasicExpirationFields(airlyticsPurchaseEvents.get(airlyticsPurchaseEvents.size() - 1),
                    lastReceiptInfo);

            int index = 1;
            for (AirlyticsPurchaseEvent airlyticsPurchaseEvent : airlyticsPurchaseEvents) {
                if (airlyticsPurchaseEvent.getName().equals(PurchaseConsumer.PurchaseEventType.RENEWED.name())) {
                    validateRenewalFields(airlyticsPurchaseEvent, ioSubscriptionPurchase.getLatestReceiptInfo().get(index));
                    index++;
                }
            }

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPurchaseNotificationRenewAfterTRIAL_START() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_RENEW.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchased-after-trial.json"),
                            IOSubscriptionPurchase.class);
            LatestReceiptInfo latestReceiptInfo = ioSubscriptionPurchase.getLatestReceiptInfo().get(0);

            latestReceiptInfo.setExpiresDateMs(System.currentTimeMillis() + 20000);
            ioSubscriptionPurchase.getPendingRenewalInfo().get(0).setAutoRenewStatus("1");

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());

            validateTrialConvertedFields(airlyticsPurchaseEvents.get(0), ioSubscriptionPurchase.getLatestReceiptInfo().get(1));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseNotificationINTERACTIVE_RENEWAL_to_Upgrade() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.INTERACTIVE_RENEWAL.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "interactive_renewal_upgrade.json"),
                            IOSubscriptionPurchase.class);
            LatestReceiptInfo latestReceiptInfo = ioSubscriptionPurchase.getLatestReceiptInfo().get(0);

            latestReceiptInfo.setExpiresDateMs(System.currentTimeMillis() + 20000);
            ioSubscriptionPurchase.getPendingRenewalInfo().get(0).setAutoRenewStatus("1");

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            AirlyticsPurchaseEvent upgraded = airlyticsPurchaseEvents.get(0);
            AirlyticsPurchaseEvent upgrade_purchased = airlyticsPurchaseEvents.get(1);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            Assert.assertEquals("260000238529455", upgraded.getPurchaseId());
            Assert.assertEquals("260000341333293", upgraded.getUpgradedTo());
            Assert.assertEquals("260000238529455", upgrade_purchased.getUpgradedFrom());
            validateBasicUpgradeFields(airlyticsPurchaseEvents.get(0), ioSubscriptionPurchase.getLatestReceiptInfo().get(1));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseNotificationINTERACTIVE_RENEWAL_to_NewPurchaseTrial() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.INTERACTIVE_RENEWAL.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "interactive_renew_to_trial.json"),
                            IOSubscriptionPurchase.class);
            LatestReceiptInfo latestReceiptInfo = ioSubscriptionPurchase.getLatestReceiptInfo().get(0);


            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            validatePurchaseTrialFields(airlyticsPurchaseEvents.get(0), latestReceiptInfo);
            validateTrialExpiredFields(airlyticsPurchaseEvents.get(1));
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPurchaseNotificationINTERACTIVE_RENEWAL_to_NewPurchase() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.INTERACTIVE_RENEWAL.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "interactive_renew_to_new_purchase.json"),
                            IOSubscriptionPurchase.class);
            LatestReceiptInfo latestReceiptInfo = ioSubscriptionPurchase.getLatestReceiptInfo().get(0);


            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());
            validatePurchaseFields(airlyticsPurchaseEvents.get(0), latestReceiptInfo);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPurchaseAppRenewsRevenueCalc() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_RENEW.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "sub_events_renewed_revenue.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase.clone(),
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPurchaseNotificationRenewDiffSubAfterTrialShouldBePurchase() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_RENEW.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "renew_diff_product_after_trial.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase.clone(),
                    metaData,
                    inAppAirlyticsProducts);

            LatestReceiptInfo latestReceiptInfo = ioSubscriptionPurchase.getLatestReceiptInfo().get(0);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());
            validatePurchaseFields(airlyticsPurchaseEvents.get(0), latestReceiptInfo);


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseNotificationMissingDID_RENEW() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_RENEW.name());


        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "missing_did_renew.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());
            validateRenewalFields(airlyticsPurchaseEvents.get(0),
                    ioSubscriptionPurchase.getLatestReceiptInfo().get(ioSubscriptionPurchase.getLatestReceiptInfo().size() - 1));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPurchaseAppRenewAfterTRIAL_START() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);


        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchased-after-trial.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            validateStartTrialBasicFields(airlyticsPurchaseEvents.get(0), ioSubscriptionPurchase.getLatestReceiptInfo().get(0));
            validateTrialConvertedFields(airlyticsPurchaseEvents.get(1), ioSubscriptionPurchase.getLatestReceiptInfo().get(1));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseFromAppCANCEL() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "canceled_sub.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(10, airlyticsPurchaseEvents.size());

            validatePurchaseFields(airlyticsPurchaseEvents.get(0), ioSubscriptionPurchase.getLatestReceiptInfo().get(0));
            long month_1Revenue = airlyticsPurchaseEvents.stream().
                    filter(event -> (event.getName().equals(PurchaseConsumer.PurchaseEventType.RENEWED.name()) ||
                            event.getName().equals(PurchaseConsumer.PurchaseEventType.PURCHASED.name()) ||
                            event.getName().equals(PurchaseConsumer.PurchaseEventType.UPGRADED.name())) &&
                            event.getProduct().equals("com.product.iap.renewing.1month.1")).
                    mapToLong(event -> event.getRevenueUsd()).sum();

            Assert.assertEquals(3070980, month_1Revenue);

            Assert.assertEquals(3070980, airlyticsPurchaseEvents.stream().
                    filter(event -> event.getPurchaseId().equals("120000191064197")
                    ).
                    mapToLong(event -> event.getRevenueUsd()).sum());


            long year_1Revenue = airlyticsPurchaseEvents.stream().
                    filter(event -> (event.getName().equals(PurchaseConsumer.PurchaseEventType.RENEWED.name()) ||
                            event.getName().equals(PurchaseConsumer.PurchaseEventType.CANCELED.name()) ||
                            event.getName().equals(PurchaseConsumer.PurchaseEventType.UPGRADE_PURCHASED.name())) &&
                            event.getProduct().equals("com.product.iap.renewing.1year.1")).
                    mapToLong(event -> event.getRevenueUsd()).sum();

            Assert.assertEquals(9990000, year_1Revenue);


            Assert.assertEquals(9990000, airlyticsPurchaseEvents.stream().
                    filter(event -> event.getPurchaseId().equals("120000214760722")
                    ).
                    mapToLong(event -> event.getRevenueUsd()).sum());

            validateTrialExpiredBasicFields(airlyticsPurchaseEvents.get(airlyticsPurchaseEvents.size() - 1),
                    ioSubscriptionPurchase.getLatestReceiptInfo().get(ioSubscriptionPurchase.getLatestReceiptInfo().size() - 1));

            validateStartTrialBasicFields(airlyticsPurchaseEvents.get(airlyticsPurchaseEvents.size() - 2),
                    ioSubscriptionPurchase.getLatestReceiptInfo().get(ioSubscriptionPurchase.getLatestReceiptInfo().size() - 1));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseFromNotificationCANCEL() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.CANCEL.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "trial_canceled.json"),
                            IOSubscriptionPurchase.class);


            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());


            validateCancelAfterTrialFields(airlyticsPurchaseEvents.get(0), ioSubscriptionPurchase.getLatestReceiptInfo().get(1));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void trialRenewShouldNotAddRevenue() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_RENEW.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "same_product_trial_after_expiration_notification.json"),
                            IOSubscriptionPurchase.class);


            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name(), airlyticsPurchaseEvents.get(0).getName());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_EXPIRED.name(), airlyticsPurchaseEvents.get(1).getName());

            Assert.assertEquals(0, airlyticsPurchaseEvents.get(0).getRevenueUsd(), 0);


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void afterTrialExpiredShouldBePurchasedAndTrialExpiredEventNotification() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.INTERACTIVE_RENEWAL.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchased_after_trial_expired.json"),
                            IOSubscriptionPurchase.class);


            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.PURCHASED.name(), airlyticsPurchaseEvents.get(0).getName());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void afterTrialExpiredShouldBePurchasedAndTrialExpiredEvent() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchased_after_trial_expired.json"),
                            IOSubscriptionPurchase.class);


            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(9, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name(), airlyticsPurchaseEvents.get(1).getName());
            Assert.assertEquals(1595259393000L, airlyticsPurchaseEvents.get(1).getEventTime(), 0);
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_EXPIRED.name(), airlyticsPurchaseEvents.get(2).getName());
            Assert.assertEquals(1595518593000L, airlyticsPurchaseEvents.get(2).getEventTime(), 0);
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.PURCHASED.name(), airlyticsPurchaseEvents.get(3).getName());
            Assert.assertEquals(1600022400000L, airlyticsPurchaseEvents.get(3).getEventTime(), 0);


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void afterTrialShouldBeTrialConvertedEventNotification() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_RENEW.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchased-after-trial.json"),
                            IOSubscriptionPurchase.class);


            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_CONVERTED.name(), airlyticsPurchaseEvents.get(0).getName());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void afterTrialShouldBeTrialConvertedEvent() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchased-after-trial.json"),
                            IOSubscriptionPurchase.class);


            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_CONVERTED.name(), airlyticsPurchaseEvents.get(1).getName());


            validateStartTrialBasicFields(airlyticsPurchaseEvents.get(0), ioSubscriptionPurchase.getLatestReceiptInfo().get(0));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void historicalRevenueBug() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "historical_revenue_bug.json"),
                            IOSubscriptionPurchase.class);


            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(3, airlyticsPurchaseEvents.size());


            validateStartTrialBasicFields(airlyticsPurchaseEvents.get(0), ioSubscriptionPurchase.getLatestReceiptInfo().get(0));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testCancelledAfterTrialShouldHaveNegativeRevenue() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.CANCEL.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "cancelled_after_trial.json"),
                            IOSubscriptionPurchase.class);
            LatestReceiptInfo latestReceiptInfo = ioSubscriptionPurchase.getLatestReceiptInfo().get(0);


            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());
            Assert.assertEquals(-9990000, airlyticsPurchaseEvents.get(0).getRevenueUsd(), 0);


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCancelledAfterRecoveredShouldHaveNegativeRevenue() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.CANCEL.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "canceled_after_recovered.json"),
                            IOSubscriptionPurchase.class);
            LatestReceiptInfo latestReceiptInfo = ioSubscriptionPurchase.getLatestReceiptInfo().get(0);


            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());
            Assert.assertEquals(-9990000, airlyticsPurchaseEvents.get(0).getRevenueUsd(), 0);


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseNotificationUPGRADE() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.CANCEL.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "to_upgraded_product.json"),
                            IOSubscriptionPurchase.class);
            LatestReceiptInfo latestReceiptInfo = ioSubscriptionPurchase.getLatestReceiptInfo().get(0);

            latestReceiptInfo.setExpiresDateMs(System.currentTimeMillis() + 20000);
            ioSubscriptionPurchase.getPendingRenewalInfo().get(0).setAutoRenewStatus("1");

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());

            validateBasicUpgradeFields(airlyticsPurchaseEvents.get(0),
                    ioSubscriptionPurchase.getLatestReceiptInfo().get(ioSubscriptionPurchase.getLatestReceiptInfo().size() - 3));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPurchaseFromAppUPGRADE2() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);


        ObjectMapper objectMapper = new ObjectMapper();
        try {

            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "subscription_event_upgrade.json"),
                            IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase.clone(),
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase.clone(),
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();

            Assert.assertNotNull(airlyticsPurchaseEvents);

            airlyticsPurchases.stream().forEach(airlyticsPurchase -> {
                Assert.assertEquals(airlyticsPurchase.getRevenueUsd().longValue(), airlyticsPurchaseEvents.stream().filter(event ->
                        event.getPurchaseId().equals(airlyticsPurchase.getId())).mapToLong(AirlyticsPurchaseEvent::getRevenueUsd).sum());
            });


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseFromAppUPGRADE() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "to_upgraded_product.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(17, airlyticsPurchaseEvents.size());
            validatePurchaseFields(airlyticsPurchaseEvents.get(0), ioSubscriptionPurchase.getLatestReceiptInfo().get(0));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void trialConvertedAfterTrialExpirationShouldNotBePurchased() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "trial_converted_after_trial_expiration.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name(), airlyticsPurchaseEvents.get(0).getName());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_CONVERTED.name(), airlyticsPurchaseEvents.get(1).getName());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }



        metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_RENEW.name());
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "trial_converted_after_trial_expiration.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_CONVERTED.name(), airlyticsPurchaseEvents.get(0).getName());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

    }


    @Test
    public void testPurchaseFromAppUPGRADERevenuePrecision() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgraded_revenue_precision.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(3, airlyticsPurchaseEvents.size());
            validatePurchaseFields(airlyticsPurchaseEvents.get(0), ioSubscriptionPurchase.getLatestReceiptInfo().get(0));
            validateBasicUpgradeFields(airlyticsPurchaseEvents.get(1), ioSubscriptionPurchase.getLatestReceiptInfo().get(0));

            Assert.assertEquals(-939060, airlyticsPurchaseEvents.get(1).getRevenueUsd().longValue());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseFromAppEXPIRED() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgrade_receipt.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(4, airlyticsPurchaseEvents.size());
            validatePurchaseFields(airlyticsPurchaseEvents.get(0), ioSubscriptionPurchase.getLatestReceiptInfo().get(0));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseWithOneTransactionFromNotificationCANCLED() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.CANCEL.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "canceled_one_transaction.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());
            validateCancelFields(airlyticsPurchaseEvents.get(0), ioSubscriptionPurchase.getLatestReceiptInfo().get(0));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPurchaseWithOneTransactionFromAppCANCLED() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);


        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "canceled_one_transaction.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            //validateCancelFields(airlyticsPurchaseEvents.get(0), ioSubscriptionPurchase.getLatestReceiptInfo().get(0));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPurchaseFromNotificationEXPIRED() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_CHANGE_RENEWAL_STATUS.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgrade_receipt.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            validateBasicExpirationFields(airlyticsPurchaseEvents.get(1), ioSubscriptionPurchase.getLatestReceiptInfo().get(1));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPurchaseFromNotificationDID_FAIL_TO_RENEW() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_FAIL_TO_RENEW.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "currently_trial2.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            validateInBillingIssueFields(airlyticsPurchaseEvents.get(0));
            validateTrialExpiredFields(airlyticsPurchaseEvents.get(1));

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPurchaseFromNotificationRecoveryShouldBeFollowedByRenewed() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_RECOVER.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "recovery_after_billing_issue.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.RECOVERED.name(), airlyticsPurchaseEvents.get(0).getName());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.RENEWED.name(), airlyticsPurchaseEvents.get(1).getName());
            Assert.assertEquals(0, airlyticsPurchaseEvents.get(0).getRevenueUsd(), 0);
            Assert.assertEquals(9990000, airlyticsPurchaseEvents.get(1).getRevenueUsd(), 0);

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPurchaseFromNotificationRecoveryShouldBeFollowedByTrialConverted() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_RECOVER.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "recovery_after_billing_issue.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.RECOVERED.name(), airlyticsPurchaseEvents.get(0).getName());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.RENEWED.name(), airlyticsPurchaseEvents.get(1).getName());
            Assert.assertEquals(0, airlyticsPurchaseEvents.get(0).getRevenueUsd(), 0);
            Assert.assertEquals(9990000, airlyticsPurchaseEvents.get(1).getRevenueUsd(), 0);

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void tempTest() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "trail_conversion_test.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(3, airlyticsPurchaseEvents.size());


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseUpgradeFromAndUpgradedToShouldBePurchaseId() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);


        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgrade-to-upgrade-from-test.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(3, airlyticsPurchaseEvents.size());
            AirlyticsPurchaseEvent upgraded = airlyticsPurchaseEvents.get(1);
            AirlyticsPurchaseEvent upgrade_purchased = airlyticsPurchaseEvents.get(2);
            Assert.assertEquals("710000197960664", upgraded.getPurchaseId());
            Assert.assertEquals("710000197960665", upgrade_purchased.getPurchaseId());
            Assert.assertEquals("710000197960665", upgraded.getUpgradedTo());
            Assert.assertEquals("710000197960664", upgrade_purchased.getUpgradedFrom());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void billingIssueFixedInTreeDaysShouldBeTrialConverted() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "billing_fixed_after_trial.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name(), airlyticsPurchaseEvents.get(0).getName());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_CONVERTED.name(), airlyticsPurchaseEvents.get(1).getName());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }


        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_RECOVER.name());
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "billing_fixed_after_trial.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.RECOVERED.name(), airlyticsPurchaseEvents.get(0).getName());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_CONVERTED.name(), airlyticsPurchaseEvents.get(1).getName());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

    }


    @Test
    public void canceledAfterTrialConverted() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);
        //metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_RENEW.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "canceled_after_trial_converted.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(3, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name(), airlyticsPurchaseEvents.get(0).getName());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_CONVERTED.name(), airlyticsPurchaseEvents.get(1).getName());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.CANCELED.name(), airlyticsPurchaseEvents.get(2).getName());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void debugTrial() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);
        //metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_CHANGE_RENEWAL_STATUS.name());



        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "debug_trial.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(8, airlyticsPurchaseEvents.size());


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void multipleCancellationBug() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_CHANGE_RENEWAL_STATUS.name());


        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "multiple_cancellation_bug.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.RENEWAL_STATUS_CHANGED.name(), airlyticsPurchaseEvents.get(0).getName());


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void recoveredShouldBeAddedAfterBillingIssueBug() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_RECOVER.name());


        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "billing_without_recovered_bug.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.RECOVERED.name(), airlyticsPurchaseEvents.get(0).getName());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.RENEWED.name(), airlyticsPurchaseEvents.get(1).getName());


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }





    @Test
    public void trialExpiredTest() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_CHANGE_RENEWAL_STATUS.name());



        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "trial_expired_bug.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_EXPIRED.name(),airlyticsPurchaseEvents.get(1).getName());


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }


        metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "trial_expired_bug.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name(),airlyticsPurchaseEvents.get(0).getName());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_EXPIRED.name(),airlyticsPurchaseEvents.get(1).getName());


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }



    }



    @Test
    public void testBillingIssueShouldAppear() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);


        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "grace_period_test.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name(), airlyticsPurchaseEvents.get(0).getName());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_EXPIRED.name(), airlyticsPurchaseEvents.get(1).getName());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.DID_FAIL_TO_RENEW.name());

        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "grace_period_test.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(2, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.BILLING_ISSUE.name(), airlyticsPurchaseEvents.get(0).getName());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_EXPIRED.name(), airlyticsPurchaseEvents.get(1).getName());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRenewAfterExpiredShouldBePurchased() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);


        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchase_after_expired.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(10, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.RENEWED.name(), airlyticsPurchaseEvents.get(9).getName());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.INTERACTIVE_RENEWAL.name());

        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchase_after_expired.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.PURCHASED.name(), airlyticsPurchaseEvents.get(0).getName());


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testInteractiveRenewalShouldBeTreatedAsNewPurchase() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);


        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "interactive_renewal_bug.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(22, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.PURCHASED.name(), airlyticsPurchaseEvents.get(21).getName());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.INTERACTIVE_RENEWAL.name());

        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.
                    readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "interactive_renewal_bug.json"),
                            IOSubscriptionPurchase.class);

            IOSAirlyticsPurchaseEvent iosAirlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts);

            List<AirlyticsPurchaseEvent> airlyticsPurchaseEvents = iosAirlyticsPurchaseEvent.getPurchaseEvents();
            Assert.assertNotNull(airlyticsPurchaseEvents);
            Assert.assertEquals(1, airlyticsPurchaseEvents.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.PURCHASED.name(), airlyticsPurchaseEvents.get(0).getName());


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    private void validateTrialExpiredFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent) {
        Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_EXPIRED.name(),
                airlyticsPurchaseEvent.getName());
        Assert.assertEquals(0,
                airlyticsPurchaseEvent.getRevenueUsd().longValue());
    }


    private void validateInBillingIssueFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent) {
        Assert.assertEquals(PurchaseConsumer.PurchaseEventType.BILLING_ISSUE.name(),
                airlyticsPurchaseEvent.getName());
        Assert.assertEquals(0,
                airlyticsPurchaseEvent.getRevenueUsd().longValue());
        Assert.assertEquals(IOSPurchaseConsumer.ExpirationReason.BILLING_ISSUE.name(),
                airlyticsPurchaseEvent.getExpirationReason());

    }

    private void validateBasicExpirationFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent, LatestReceiptInfo latestReceiptInfo) {
        Assert.assertEquals(PurchaseConsumer.PurchaseEventType.EXPIRED.name(),
                airlyticsPurchaseEvent.getName());
        Assert.assertEquals(latestReceiptInfo.getExpiresDateMs(),
                airlyticsPurchaseEvent.getEventTime());
        Assert.assertEquals(0,
                airlyticsPurchaseEvent.getRevenueUsd().longValue());

    }

    private void validateBasicUpgradeFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent, LatestReceiptInfo latestReceiptInfo) {
        Assert.assertEquals(PurchaseConsumer.PurchaseEventType.UPGRADED.name(),
                airlyticsPurchaseEvent.getName());
        Assert.assertEquals(latestReceiptInfo.getCancellationDateMs(),
                airlyticsPurchaseEvent.getEventTime().longValue(), 0);
    }


    private void validateCancelFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent, LatestReceiptInfo latestReceiptInfo) {
        Assert.assertEquals(PurchaseConsumer.PurchaseEventType.CANCELED.name(),
                airlyticsPurchaseEvent.getName());
        Assert.assertEquals(latestReceiptInfo.getCancellationDateMs(),
                airlyticsPurchaseEvent.getEventTime());
        Assert.assertEquals(airlyticsPurchaseEvent.getRevenueUsd(),
                -(inAppAirlyticsProducts.get(airlyticsPurchaseEvent.getProduct()).getPriceUSDMicros()), 0);
    }

    private void validateCancelAfterTrialFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent, LatestReceiptInfo latestReceiptInfo) {
        Assert.assertEquals(PurchaseConsumer.PurchaseEventType.CANCELED.name(),
                airlyticsPurchaseEvent.getName());
        Assert.assertEquals(latestReceiptInfo.getCancellationDateMs(),
                airlyticsPurchaseEvent.getEventTime());
        Assert.assertEquals(-9990000,
                airlyticsPurchaseEvent.getRevenueUsd().longValue(), 0);
    }


    private void validateTrialConvertedFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent, LatestReceiptInfo latestReceiptInfo) {
        Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_CONVERTED.name(),
                airlyticsPurchaseEvent.getName());
        Assert.assertEquals(latestReceiptInfo.getPurchaseDateMs(),
                airlyticsPurchaseEvent.getEventTime());
        Assert.assertEquals(inAppAirlyticsProducts.get(airlyticsPurchaseEvent.getProduct()).getPriceUSDMicros(),
                airlyticsPurchaseEvent.getRevenueUsd());
    }


    private void validateRenewalFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent, LatestReceiptInfo latestReceiptInfo) {
        Assert.assertEquals(PurchaseConsumer.PurchaseEventType.RENEWED.name(),
                airlyticsPurchaseEvent.getName());
        Assert.assertEquals(latestReceiptInfo.getPurchaseDateMs(),
                airlyticsPurchaseEvent.getEventTime());
        Assert.assertEquals(inAppAirlyticsProducts.get(airlyticsPurchaseEvent.getProduct()).getPriceUSDMicros(),
                airlyticsPurchaseEvent.getRevenueUsd());
    }

    private void validateTrialExpiredBasicFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent, LatestReceiptInfo latestReceiptInfo) {
        Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_EXPIRED.name(),
                airlyticsPurchaseEvent.getName());
        Assert.assertEquals(latestReceiptInfo.getExpiresDateMs(),
                airlyticsPurchaseEvent.getEventTime());
        Assert.assertEquals(0.0,
                airlyticsPurchaseEvent.getRevenueUsd().longValue(), 0);

    }

    private void validateStartTrialBasicFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent, LatestReceiptInfo latestReceiptInfo) {
        Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name(),
                airlyticsPurchaseEvent.getName());
        Assert.assertEquals(latestReceiptInfo.getPurchaseDateMs(),
                airlyticsPurchaseEvent.getEventTime());
        Assert.assertEquals(latestReceiptInfo.getExpiresDateMs(),
                airlyticsPurchaseEvent.getExpirationDate());
        Assert.assertEquals(0.0,
                airlyticsPurchaseEvent.getRevenueUsd().longValue(), 0);

    }

    private void validatePurchaseTrialFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent, LatestReceiptInfo latestReceiptInfo) {
        Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_STARTED.name(),
                airlyticsPurchaseEvent.getName());
        Assert.assertEquals(latestReceiptInfo.getPurchaseDateMs(),
                airlyticsPurchaseEvent.getEventTime());
        Assert.assertEquals(latestReceiptInfo.getExpiresDateMs(),
                airlyticsPurchaseEvent.getExpirationDate());
        Assert.assertEquals(0L,
                airlyticsPurchaseEvent.getRevenueUsd().longValue());

    }

    private void validatePurchaseFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent, LatestReceiptInfo latestReceiptInfo) {
        Assert.assertEquals(PurchaseConsumer.PurchaseEventType.PURCHASED.name(),
                airlyticsPurchaseEvent.getName());
        Assert.assertEquals(latestReceiptInfo.getPurchaseDateMs(),
                airlyticsPurchaseEvent.getEventTime());
        Assert.assertEquals(latestReceiptInfo.getExpiresDateMs(),
                airlyticsPurchaseEvent.getExpirationDate());
        Assert.assertEquals(inAppAirlyticsProducts.get(airlyticsPurchaseEvent.getProduct()).getPriceUSDMicros(),
                airlyticsPurchaseEvent.getRevenueUsd());

    }

    private String readTestReceipt(String path) throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(path)) {
            return IOUtils.toString(Objects.requireNonNull(inputStream), StandardCharsets.UTF_8);
        }
    }
}