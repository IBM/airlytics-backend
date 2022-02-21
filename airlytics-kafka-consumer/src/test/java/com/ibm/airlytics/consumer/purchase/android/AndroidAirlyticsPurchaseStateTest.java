package com.ibm.airlytics.consumer.purchase.android;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.androidpublisher.model.IntroductoryPriceInfo;
import com.google.api.services.androidpublisher.model.SubscriptionCancelSurveyResult;
import com.google.api.services.androidpublisher.model.SubscriptionPurchase;
import com.ibm.airlytics.consumer.purchase.AirlyticsInAppProduct;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;
import com.ibm.airlytics.consumer.purchase.PurchaseConsumer;
import com.ibm.airlytics.consumer.purchase.events.*;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SubscriptionPurchase.class, SubscriptionCancelSurveyResult.class, IntroductoryPriceInfo.class})
@PowerMockIgnore({"javax.xml.*"})
public class AndroidAirlyticsPurchaseStateTest {
    protected static final String TEST_DATE_FOLDER = "android_consumer_test_data";
    protected static final long maxSubscriptionEventAge = 31536000000L;

    @SuppressWarnings({"SpellCheckingInspection"})
    protected static final String inAppProducts = "{\"com.product.iap.renewing.1month.pro\":{\"priceUSDMicros\":4990000,\"gracePeriod\":\"P1W\",\"productId\":\"com.product.iap.renewing.1month.pro\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"adfree.annual.30off\":{\"priceUSDMicros\":9990000,\"gracePeriod\":\"P1W\",\"productId\":\"adfree.annual.30off\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"adfree.annual.50off\":{\"priceUSDMicros\":9990000,\"gracePeriod\":\"P1W\",\"productId\":\"adfree.annual.50off\",\"trialPeriod\":\"P3D\",\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1week.1\":{\"priceUSDMicros\":990000,\"gracePeriod\":\"P1W\",\"productId\":\"com.product.iap.renewing.1week.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1W\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.pro\":{\"priceUSDMicros\":29990000,\"gracePeriod\":\"P1W\",\"productId\":\"com.product.iap.renewing.1year.pro\",\"trialPeriod\":\"P1W\",\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"product.adfree\":{\"priceUSDMicros\":990000,\"gracePeriod\":\"P1W\",\"productId\":\"product.adfree\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.1\":{\"priceUSDMicros\":9990000,\"gracePeriod\":\"P1W\",\"productId\":\"com.product.iap.renewing.1year.1\",\"trialPeriod\":\"P1W\",\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":false,\"introPriceUSDMicros\":null}}";
    protected HashMap<String, AirlyticsInAppProduct> inAppAirlyticsProducts;


    protected static final ObjectMapper objectMapper = new ObjectMapper();
    PurchaseConsumer purchaseConsumer;

    @Before
    public void setup() {
        try {
            purchaseConsumer = new PurchaseConsumer() {
                @Override
                protected void refreshInAppsList() {

                }

                @Override
                protected int processRecords(ConsumerRecords<String, JsonNode> records) {
                    return 0;
                }
            };
            inAppAirlyticsProducts = objectMapper.readValue(inAppProducts, new TypeReference<HashMap<String, AirlyticsInAppProduct>>() {
            });
        } catch (JsonProcessingException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSurveyResponseShouldNotBeNull() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "survey_response_test.json"));
            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertNotNull(airlyticsPurchases.getSurveyResponse());
            Assert.assertEquals(PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus(),
                    airlyticsPurchases.getSubscriptionStatus());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testInTrialInPendingPaymentStateShouldHavePeriodZero() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "trial_in_pending_payment.json"));

            AirlyticsPurchase previousAirlyticsPurchaseState = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.pro")
            ).getUpdatedPurchase();
            previousAirlyticsPurchaseState.setTrialStartDate(System.currentTimeMillis());

            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.pro"),
                    previousAirlyticsPurchaseState,
                    null
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(0, airlyticsPurchases.getPeriods().doubleValue(), 0);

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testActivePurchaseShouldActiveBeTrue2() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "active-purchase2.json"));
            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.pro")
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(true, airlyticsPurchases.getActive());
            Assert.assertEquals(1, airlyticsPurchases.getPeriods().doubleValue(), 0);
            Assert.assertEquals(airlyticsPurchases.getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.ACTIVE.getStatus());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testActivePurchaseShouldActiveBeTrue() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "active-purchase.json"));
            subscriptionPurchaseHashRawObject.put("expiryTimeMillis", (System.currentTimeMillis() + 500000) + "");
            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("product.adfree")
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(true, airlyticsPurchases.getActive());
            Assert.assertEquals(1, airlyticsPurchases.getPeriods().doubleValue(), 0);
            Assert.assertEquals(airlyticsPurchases.getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.ACTIVE.getStatus());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRevenueCalculationShouldNotBeZero() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "revenue-calculation.json"));
            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testSubscriptionExpiredEvent() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        try {
            JSONObject updatedAirlyticsPurchaseJSON = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "expired-purchase.json"));
            JSONObject currentAirlyticsPurchaseJSON = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "before-expired-purchase.json"));


            SubscriptionPurchase updatedAirlyticsPurchaseSub = createMockedSubscriptionPurchase(updatedAirlyticsPurchaseJSON);
            AirlyticsPurchase updatedAirlyticsPurchase = new AndroidAirlyticsPurchaseState(
                    updatedAirlyticsPurchaseSub,
                    metaData,
                    inAppAirlyticsProducts.get("product.adfree")
            ).getUpdatedPurchase();

            SubscriptionPurchase createMockedSubscriptionPurchaseSub = createMockedSubscriptionPurchase(currentAirlyticsPurchaseJSON);
            createMockedSubscriptionPurchaseSub.setPurchaseType(null);
            AirlyticsPurchase currentAirlyticsPurchase = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchaseSub,
                    metaData,
                    inAppAirlyticsProducts.get("product.adfree")
            ).getUpdatedPurchase();

            Assert.assertNotNull(updatedAirlyticsPurchase);
            Assert.assertNotNull(currentAirlyticsPurchase);

            currentAirlyticsPurchase.setUserId("");
            currentAirlyticsPurchase.setActive(true);
            currentAirlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get("product.adfree").getPriceUSDMicros());

            try {
                List<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(updatedAirlyticsPurchase,
                        currentAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                        null, "android", maxSubscriptionEventAge);

                Assert.assertEquals(2, (subscriptionEvents).size());

                SubscriptionExpiredEvent subscriptionExpiredEvent = (SubscriptionExpiredEvent) subscriptionEvents.get(0);
                UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(1);

            } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                illegalSubscriptionEventArguments.printStackTrace();
            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSubscriptionExpiredCancelEvent() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        try {
            JSONObject updatedAirlyticsPurchaseJSON = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "expired-purchase-canceled.json"));
            JSONObject currentAirlyticsPurchaseJSON = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "before-expired-purchase1.json"));


            SubscriptionPurchase updatedAirlyticsPurchaseSub = createMockedSubscriptionPurchase(updatedAirlyticsPurchaseJSON);
            AirlyticsPurchase updatedAirlyticsPurchase = new AndroidAirlyticsPurchaseState(
                    updatedAirlyticsPurchaseSub,
                    metaData,
                    inAppAirlyticsProducts.get("product.adfree")
            ).getUpdatedPurchase();

            SubscriptionPurchase createMockedSubscriptionPurchaseSub = createMockedSubscriptionPurchase(currentAirlyticsPurchaseJSON);
            createMockedSubscriptionPurchaseSub.setPurchaseType(null);
            AirlyticsPurchase currentAirlyticsPurchase = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchaseSub,
                    metaData,
                    inAppAirlyticsProducts.get("product.adfree")
            ).getUpdatedPurchase();

            Assert.assertNotNull(updatedAirlyticsPurchase);
            Assert.assertNotNull(currentAirlyticsPurchase);

            currentAirlyticsPurchase.setUserId("");
            currentAirlyticsPurchase.setActive(true);
            updatedAirlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get("product.adfree").getPriceUSDMicros());
            currentAirlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get("product.adfree").getPriceUSDMicros());

            try {
                List<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(updatedAirlyticsPurchase,
                        currentAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                        null, "android", maxSubscriptionEventAge);

                Assert.assertEquals(3, (subscriptionEvents).size());

                SubscriptionExpiredEvent subscriptionExpiredEvent = (SubscriptionExpiredEvent) subscriptionEvents.get(0);
                SubscriptionCancelledEvent subscriptionCancelledEvent = (SubscriptionCancelledEvent) subscriptionEvents.get(1);
                UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(2);

            } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                illegalSubscriptionEventArguments.printStackTrace();
            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testSubscriptionExpiredCancelEvent2() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        try {
            JSONObject updatedAirlyticsPurchaseJSON = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "expired-system-after.json"));
            JSONObject currentAirlyticsPurchaseJSON = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "expired-system-before.json"));


            SubscriptionPurchase updatedAirlyticsPurchaseSub = createMockedSubscriptionPurchase(updatedAirlyticsPurchaseJSON);
            AirlyticsPurchase updatedAirlyticsPurchase = new AndroidAirlyticsPurchaseState(
                    updatedAirlyticsPurchaseSub,
                    metaData,
                    inAppAirlyticsProducts.get("product.adfree")
            ).getUpdatedPurchase();

            SubscriptionPurchase createMockedSubscriptionPurchaseSub = createMockedSubscriptionPurchase(currentAirlyticsPurchaseJSON);
            createMockedSubscriptionPurchaseSub.setPurchaseType(null);
            AirlyticsPurchase currentAirlyticsPurchase = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchaseSub,
                    metaData,
                    inAppAirlyticsProducts.get("product.adfree")
            ).getUpdatedPurchase();

            Assert.assertNotNull(updatedAirlyticsPurchase);
            Assert.assertNotNull(currentAirlyticsPurchase);

            currentAirlyticsPurchase.setUserId("");
            currentAirlyticsPurchase.setActive(true);
            updatedAirlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get("product.adfree").getPriceUSDMicros());
            currentAirlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get("product.adfree").getPriceUSDMicros());

            try {
                List<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(updatedAirlyticsPurchase,
                        currentAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                        null, "android", maxSubscriptionEventAge);

                Assert.assertEquals(3, (subscriptionEvents).size());

                SubscriptionExpiredEvent subscriptionExpiredEvent = (SubscriptionExpiredEvent) subscriptionEvents.get(0);
                SubscriptionCancelledEvent subscriptionCancelledEvent = (SubscriptionCancelledEvent) subscriptionEvents.get(1);
                UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(2);

            } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                illegalSubscriptionEventArguments.printStackTrace();
            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void subInTrialPeriodWithPaymentStateOneShouldBeRelyOnStartEndDates() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "sub-in-trial-period-without-payment-state-1.json"));

            AirlyticsPurchase previousAirlyticsPurchasesState = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.pro")
            ).getUpdatedPurchase();

            previousAirlyticsPurchasesState.setTrialStartDate(System.currentTimeMillis());


            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.pro"),
                    previousAirlyticsPurchasesState,
                    null
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(true, airlyticsPurchases.getTrial());
            Assert.assertNotNull(airlyticsPurchases.getTrialStartDate());
            Assert.assertNotNull(airlyticsPurchases.getTrialEndDate());
            Assert.assertEquals(0L, airlyticsPurchases.getPeriods(), 0);
            Assert.assertEquals(0L, airlyticsPurchases.getRevenueUsd(), 0);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }


        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "sub-in-trial-period-without-payment-cancelled.json"));
            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.pro")
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(false, airlyticsPurchases.getTrial());
            Assert.assertNull(airlyticsPurchases.getTrialStartDate());
            Assert.assertNull(airlyticsPurchases.getTrialEndDate());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testRenewalPeriodStartDate() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "renewal-period-start-date.json"));
            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(true, airlyticsPurchases.getActive());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void periodShouldNotBeIncreasedIdPaymentStateIsNotPaymentReceived() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new
                    JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "duplicates-expired-events.json"));
            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(false, airlyticsPurchases.getActive());
            Assert.assertEquals(1, airlyticsPurchases.getPeriods().doubleValue(), 0);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPeriodSystemCanceledSub() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "system-canceled-sub.json"));
            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("product.adfree")
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(5.0, airlyticsPurchases.getPeriods().doubleValue(), 0);
            Assert.assertEquals(airlyticsPurchases.getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.CANCELED.getStatus());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testOnHoldSubStatus() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(AndroidPurchaseConsumer.NotificationType.ON_HOLD.name());

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "expired-system-before.json"));
            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("product.adfree")
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(8.0, airlyticsPurchases.getPeriods().doubleValue(), 0);
            Assert.assertEquals(airlyticsPurchases.getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.ON_HOLD.getStatus());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testOnHoldSubStatusWithBillingPeriod() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(AndroidPurchaseConsumer.NotificationType.ON_HOLD.name());

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "sub-onhold-one-billing-period.json"));
            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("product.adfree")
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(1.0, airlyticsPurchases.getPeriods().doubleValue(), 0);
            Assert.assertEquals(airlyticsPurchases.getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.ON_HOLD.getStatus());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testExpiredSubAfterFirstBillingPeriod() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(AndroidPurchaseConsumer.NotificationType.EXPIRED.name());

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "expired-after-first-billing-period.json"));
            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("product.adfree")
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(1.0, airlyticsPurchases.getPeriods().doubleValue(), 0);
            Assert.assertEquals(airlyticsPurchases.getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.CANCELED.getStatus());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void expiredAfterTwoPeriodsShouldHavePeriodTwo() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);


        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "expired-after-two-period.json"));
            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("product.adfree")
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(2.0, airlyticsPurchases.getPeriods().doubleValue(), 0);

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testOnPauseSubStatus() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(AndroidPurchaseConsumer.NotificationType.PAUSED.name());

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "expired-system-before.json"));
            AirlyticsPurchase airlyticsPurchases = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("product.adfree")
            ).getUpdatedPurchase();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(8.0, airlyticsPurchases.getPeriods().doubleValue(), 0);
            Assert.assertEquals(airlyticsPurchases.getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.PAUSED.getStatus());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    protected String readTestPurchase(String path) throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(path)) {
            return IOUtils.toString(Objects.requireNonNull(inputStream), StandardCharsets.UTF_8);
        }
    }

    protected SubscriptionPurchase createMockedSubscriptionPurchase(JSONObject subscriptionPurchaseHashRawObject) {
        SubscriptionPurchase subscriptionPurchase = PowerMockito.mock(SubscriptionPurchase.class);
        SubscriptionCancelSurveyResult subscriptionCancelSurveyResult = PowerMockito.mock(SubscriptionCancelSurveyResult.class);

        when(subscriptionPurchase.getPaymentState()).thenReturn(subscriptionPurchaseHashRawObject.has("paymentState") ?
                subscriptionPurchaseHashRawObject.getInt("paymentState") : null);
        when(subscriptionPurchase.getOrderId()).thenReturn(subscriptionPurchaseHashRawObject.has("orderId") ?
                subscriptionPurchaseHashRawObject.getString("orderId") : null);
        when(subscriptionPurchase.getAutoRenewing()).thenReturn(subscriptionPurchaseHashRawObject.getBoolean("autoRenewing"));
        when(subscriptionPurchase.getPurchaseType()).thenReturn(null);
        when(subscriptionPurchase.getExpiryTimeMillis()).thenReturn(subscriptionPurchaseHashRawObject.getLong("expiryTimeMillis"));
        when(subscriptionPurchase.getCancelReason()).thenReturn(subscriptionPurchaseHashRawObject.has("cancelReason") ?
                subscriptionPurchaseHashRawObject.getInt("cancelReason") : null);
        when(subscriptionPurchase.getCountryCode()).thenReturn(subscriptionPurchaseHashRawObject.getString("countryCode"));
        when(subscriptionPurchase.getStartTimeMillis()).thenReturn(subscriptionPurchaseHashRawObject.getLong("startTimeMillis"));
        when(subscriptionPurchase.getUserCancellationTimeMillis()).thenReturn(subscriptionPurchaseHashRawObject.has("userCancellationTimeMillis") ?
                subscriptionPurchaseHashRawObject.getLong("userCancellationTimeMillis") : null);
        when(subscriptionCancelSurveyResult.getCancelSurveyReason()).thenReturn(subscriptionPurchaseHashRawObject.has("cancelSurveyResult")
                && subscriptionPurchaseHashRawObject.getJSONObject("cancelSurveyResult").has("cancelSurveyReason") ?
                subscriptionPurchaseHashRawObject.getJSONObject("cancelSurveyResult").getInt("cancelSurveyReason") : null);
        when(subscriptionPurchase.getCancelSurveyResult()).thenReturn(subscriptionCancelSurveyResult);
        return subscriptionPurchase;
    }
}