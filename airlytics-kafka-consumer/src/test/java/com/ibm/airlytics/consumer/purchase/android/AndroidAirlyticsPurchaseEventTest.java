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
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchaseEvent;
import com.ibm.airlytics.consumer.purchase.PurchaseConsumer;
import junit.framework.TestCase;
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
import java.util.*;

import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SubscriptionPurchase.class, SubscriptionCancelSurveyResult.class, IntroductoryPriceInfo.class})
@PowerMockIgnore({"javax.xml.*"})
public class AndroidAirlyticsPurchaseEventTest extends TestCase {


    private static final String TEST_PURCHASE_TOKEN = "lgjadplfoaoegejbdojfhgfl.AO-J1OxqIygVESdajGskvpu1dC1XAr0qo63ECTipv83Srb70gbfFNqx40mYKWi-w_Zh4c2aV07mdNpq1F0icobVmKcKEyNOnQsPROuUI-aJyxhs-1uUoX2Hn_y_xcJHWrhQJqeRdEtyq";
    private static final String TEST_DATE_FOLDER = "android_consumer_test_data";

    @SuppressWarnings({"SpellCheckingInspection"})
    private static final String inAppProducts = "{\"com.product.iap.renewing.1month.pro\":{\"priceUSDMicros\":4990000,\"gracePeriod\":\"P1W\",\"productId\":\"com.product.iap.renewing.1month.pro\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"adfree.annual.30off\":{\"priceUSDMicros\":9990000,\"gracePeriod\":\"P1W\",\"productId\":\"adfree.annual.30off\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"adfree.annual.50off\":{\"priceUSDMicros\":9990000,\"gracePeriod\":\"P1W\",\"productId\":\"adfree.annual.50off\",\"trialPeriod\":\"P3D\",\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1week.1\":{\"priceUSDMicros\":990000,\"gracePeriod\":\"P1W\",\"productId\":\"com.product.iap.renewing.1week.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1W\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.pro\":{\"priceUSDMicros\":29990000,\"gracePeriod\":\"P1W\",\"productId\":\"com.product.iap.renewing.1year.pro\",\"trialPeriod\":\"P1W\",\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"product.adfree\":{\"priceUSDMicros\":990000,\"gracePeriod\":\"P1W\",\"productId\":\"product.adfree\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.1\":{\"priceUSDMicros\":9990000,\"gracePeriod\":\"P1W\",\"productId\":\"com.product.iap.renewing.1year.1\",\"trialPeriod\":\"P1W\",\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":false,\"introPriceUSDMicros\":null}}";
    private HashMap<String, AirlyticsInAppProduct> inAppAirlyticsProducts;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    PurchaseConsumer purchaseConsumer;
    ArrayList<AirlyticsPurchaseEvent> oneHistoricalAirlyticsPurchaseEvent = new ArrayList();


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


            AirlyticsPurchaseEvent airlyticsPurchaseEvent = new AirlyticsPurchaseEvent();
            airlyticsPurchaseEvent.setName(PurchaseConsumer.PurchaseEventType.RENEWED.name());
            airlyticsPurchaseEvent.setRevenueUsd(4990000L);
            oneHistoricalAirlyticsPurchaseEvent.add(airlyticsPurchaseEvent);
        } catch (JsonProcessingException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPurchaseEventOfExpiredSubFromHistory() {
        long eventTime = System.currentTimeMillis();
        PurchaseConsumer.PurchaseMetaData metaData = createMockedMetaDataFromAPP(eventTime);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "expired-purchase.json"));
            SubscriptionPurchase subscriptionPurchase = createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject);
            List<AirlyticsPurchaseEvent> airlyticsPurchases = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getPurchaseEventFromRenewalsHistory();


            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(2, airlyticsPurchases.size());

            validateBasicFields(airlyticsPurchases.get(0), subscriptionPurchase.getStartTimeMillis()
                    , PurchaseConsumer.PurchaseEventType.PURCHASED.name(),
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1").getPriceUSDMicros());

            validateBasicFields(airlyticsPurchases.get(1), subscriptionPurchase.getExpiryTimeMillis()
                    , PurchaseConsumer.PurchaseEventType.EXPIRED.name(),
                    0);

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPurchaseEventOfSubFromPURCHASENotification() {
        long eventTime = System.currentTimeMillis();
        PurchaseConsumer.PurchaseMetaData metaData = createMockedMetaDataFromNotification(eventTime,
                AndroidPurchaseConsumer.NotificationType.PURCHASED);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "expired-purchase.json"));
            SubscriptionPurchase subscriptionPurchase = createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject);
            List<AirlyticsPurchaseEvent> airlyticsPurchases = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getPurchaseEventTriggeredByNotification();


            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(1, airlyticsPurchases.size());

            validateBasicFields(airlyticsPurchases.get(0), subscriptionPurchase.getStartTimeMillis()
                    , PurchaseConsumer.PurchaseEventType.PURCHASED.name(),
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1").getPriceUSDMicros());


        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testREPLACED_WITH_SUBOneYearOneUpgradedToPro() {
        long eventTime = System.currentTimeMillis();
        PurchaseConsumer.PurchaseMetaData metaData = createMockedMetaDataFromNotification(eventTime,
                AndroidPurchaseConsumer.NotificationType.EXPIRED);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "one-year-one-upgraded-to-pro.json"));
            SubscriptionPurchase subscriptionPurchase = createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject);

            AirlyticsPurchase currentState = new AndroidAirlyticsPurchaseState(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getUpdatedPurchase();

            currentState.setPeriodStartDate(1610757559000L);

            List<AirlyticsPurchaseEvent> airlyticsPurchases = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1"),
                    currentState
            ).getPurchaseEventTriggeredByNotification();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(1, airlyticsPurchases.size());

            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.UPGRADED.name(), airlyticsPurchases.get(0).getName());
            Assert.assertEquals(airlyticsPurchases.get(0).getRevenueUsd(), -2727270L, 0L);

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseEventOfSubFromRECOVEREDNotification() {
        long eventTime = System.currentTimeMillis();
        PurchaseConsumer.PurchaseMetaData metaData = createMockedMetaDataFromNotification(eventTime,
                AndroidPurchaseConsumer.NotificationType.RECOVERED);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "expired-purchase.json"));
            SubscriptionPurchase subscriptionPurchase = createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject);
            List<AirlyticsPurchaseEvent> airlyticsPurchases = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getPurchaseEventTriggeredByNotification();

            AirlyticsPurchase airlyticsPurchase = new AndroidAirlyticsPurchaseState(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getUpdatedPurchase();


            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(2, airlyticsPurchases.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.RECOVERED.name(), airlyticsPurchases.get(1).getName());
            validateBasicFields(airlyticsPurchases.get(0), metaData.getNotificationTime()
                    , PurchaseConsumer.PurchaseEventType.RENEWED.name(),
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1").getPriceUSDMicros());

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void trialExpiredBug() {
        long eventTime = System.currentTimeMillis();
        PurchaseConsumer.PurchaseMetaData metaData = createMockedMetaDataFromNotification(eventTime,
                AndroidPurchaseConsumer.NotificationType.EXPIRED);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "trial-expired-bug.json"));
            SubscriptionPurchase subscriptionPurchase = createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject);

            AirlyticsPurchase airlyticsPurchase = new AndroidAirlyticsPurchaseState(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getUpdatedPurchase();

            airlyticsPurchase.setTrialStartDate(System.currentTimeMillis());
            airlyticsPurchase.setPeriods(0.0);

            List<AirlyticsPurchaseEvent> airlyticsPurchases = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1"),
                    airlyticsPurchase, null
            ).getPurchaseEventTriggeredByNotification();


            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(1, airlyticsPurchases.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.TRIAL_EXPIRED.name(), airlyticsPurchases.get(0).getName());


        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPurchaseEventOfSubFromRECOVEREDNotification2() {
        long eventTime = System.currentTimeMillis();
        PurchaseConsumer.PurchaseMetaData metaData = createMockedMetaDataFromNotification(eventTime,
                AndroidPurchaseConsumer.NotificationType.RECOVERED);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "recovered-purchase.json"));
            SubscriptionPurchase subscriptionPurchase = createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject);
            List<AirlyticsPurchaseEvent> airlyticsPurchases = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1month.pro")
            ).getPurchaseEventTriggeredByNotification();

            AirlyticsPurchase airlyticsPurchase = new AndroidAirlyticsPurchaseState(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1month.pro")
            ).getUpdatedPurchase();


            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(2, airlyticsPurchases.size());
            Assert.assertEquals(PurchaseConsumer.PurchaseEventType.RECOVERED.name(), airlyticsPurchases.get(1).getName());
            validateBasicFields(airlyticsPurchases.get(0), metaData.getNotificationTime()
                    , PurchaseConsumer.PurchaseEventType.RENEWED.name(),
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1month.pro").getPriceUSDMicros());

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseEventOfSubFromCANCELEDNotification() {
        long eventTime = System.currentTimeMillis();
        PurchaseConsumer.PurchaseMetaData metaData = createMockedMetaDataFromNotification(eventTime,
                AndroidPurchaseConsumer.NotificationType.CANCELED);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "expired-purchase.json"));
            SubscriptionPurchase subscriptionPurchase = createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject);
            List<AirlyticsPurchaseEvent> airlyticsPurchases = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getPurchaseEventTriggeredByNotification();


            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(1, airlyticsPurchases.size());

            validateBasicFields(airlyticsPurchases.get(0), eventTime
                    , PurchaseConsumer.PurchaseEventType.RENEWAL_STATUS_CHANGED.name(),
                    0);

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testDoubleExpirationsNotification() {
        long eventTime = System.currentTimeMillis();
        PurchaseConsumer.PurchaseMetaData metaData = createMockedMetaDataFromNotification(eventTime,
                AndroidPurchaseConsumer.NotificationType.CANCELED);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "duplicates-expired-events.json"));
            SubscriptionPurchase subscriptionPurchase = createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject);
            List<AirlyticsPurchaseEvent> airlyticsPurchases = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getPurchaseEventTriggeredByNotification();


            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(1, airlyticsPurchases.size());

            validateBasicFields(airlyticsPurchases.get(0), eventTime
                    , PurchaseConsumer.PurchaseEventType.RENEWAL_STATUS_CHANGED.name(),
                    0);

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        metaData = createMockedMetaDataFromAPP(eventTime);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "duplicates-expired-events.json"));
            SubscriptionPurchase subscriptionPurchase = createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject);
            List<AirlyticsPurchaseEvent> airlyticsPurchases = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getPurchaseEventFromRenewalsHistory();


            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(2, airlyticsPurchases.size());

            validateBasicFields(airlyticsPurchases.get(0), 1625607194424L
                    , PurchaseConsumer.PurchaseEventType.PURCHASED.name(),
                    9990000L);

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseEventOfSubFromRENEWNotification() {
        long eventTime = System.currentTimeMillis();
        PurchaseConsumer.PurchaseMetaData metaData = createMockedMetaDataFromNotification(eventTime,
                AndroidPurchaseConsumer.NotificationType.RENEWED);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "renewal-period-start-date.json"));
            SubscriptionPurchase subscriptionPurchase = createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject);
            List<AirlyticsPurchaseEvent> airlyticsPurchases = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getPurchaseEventTriggeredByNotification();


            AirlyticsPurchase airlyticsPurchase = new AndroidAirlyticsPurchaseState(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1")
            ).getUpdatedPurchase();


            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(1, airlyticsPurchases.size());

            validateBasicFields(airlyticsPurchases.get(0), airlyticsPurchase.getPeriodStartDate()
                    , PurchaseConsumer.PurchaseEventType.RENEWED.name(),
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.1").getPriceUSDMicros());

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testTrialConverted() {
        long eventTime = System.currentTimeMillis();
        PurchaseConsumer.PurchaseMetaData metaData = createMockedMetaDataFromNotification(eventTime,
                AndroidPurchaseConsumer.NotificationType.RENEWED);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "trial_converted_1.json"));
            SubscriptionPurchase subscriptionPurchase = createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject);
            AirlyticsPurchase currentState = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.pro")
            ).getUpdatedPurchase();
            currentState.setTrial(true);
            List<AirlyticsPurchaseEvent> airlyticsPurchases = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.pro"),
                    currentState
            ).getPurchaseEventTriggeredByNotification();

            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(2.999E7, airlyticsPurchases.get(0).getRevenueUsd(), 0);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseAndTrialShouldNotAppearAtTheSameTime() {
        long eventTime = System.currentTimeMillis();
        PurchaseConsumer.PurchaseMetaData metaData = createMockedMetaDataFromNotification(eventTime,
                AndroidPurchaseConsumer.NotificationType.REVOKED);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "trial-revoked.json"));
            SubscriptionPurchase subscriptionPurchase = createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject);
            List<AirlyticsPurchaseEvent> airlyticsPurchases = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1year.pro")
            ).getPurchaseEventTriggeredByNotification();


            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(0L, airlyticsPurchases.get(0).getRevenueUsd(), 0);

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testPurchaseCanceledShouldBeRefunded() {
        long eventTime = System.currentTimeMillis();
        PurchaseConsumer.PurchaseMetaData metaData = createMockedMetaDataFromNotification(eventTime,
                AndroidPurchaseConsumer.NotificationType.REVOKED);

        try {
            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "canceled-should-be-refunded.json"));
            SubscriptionPurchase subscriptionPurchase = createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject);

            AirlyticsPurchaseEvent airlyticsPurchaseEventCANCELED = new AirlyticsPurchaseEvent();
            airlyticsPurchaseEventCANCELED.setName(PurchaseConsumer.PurchaseEventType.CANCELED.name());
            oneHistoricalAirlyticsPurchaseEvent.add(airlyticsPurchaseEventCANCELED);

            AirlyticsPurchaseEvent airlyticsPurchaseEventPURCHASE = new AirlyticsPurchaseEvent();
            airlyticsPurchaseEventPURCHASE.setName(PurchaseConsumer.PurchaseEventType.PURCHASED.name());
            airlyticsPurchaseEventPURCHASE.setEventTime(System.currentTimeMillis());
            oneHistoricalAirlyticsPurchaseEvent.add(airlyticsPurchaseEventPURCHASE);


            AirlyticsPurchase currentState = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1month.pro"),
                    null,
                    oneHistoricalAirlyticsPurchaseEvent
            ).getUpdatedPurchase();


            List<AirlyticsPurchaseEvent> airlyticsPurchases = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts.get("com.product.iap.renewing.1month.pro"),
                    currentState,
                    oneHistoricalAirlyticsPurchaseEvent
            ).getPurchaseEventTriggeredByNotification();


            Assert.assertNotNull(airlyticsPurchases);
            Assert.assertEquals(-4990000L, airlyticsPurchases.get(0).getRevenueUsd(), 0);
            Assert.assertEquals(1, currentState.getPeriods(), 0);

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    private void validateBasicFields(AirlyticsPurchaseEvent airlyticsPurchaseEvent, long time, String eventType, long revenue) {
        Assert.assertEquals(eventType, airlyticsPurchaseEvent.getName());
        Assert.assertEquals(time,
                airlyticsPurchaseEvent.getEventTime(), 0);
        Assert.assertEquals(TEST_PURCHASE_TOKEN,
                airlyticsPurchaseEvent.getPurchaseId());
        Assert.assertEquals(revenue,
                airlyticsPurchaseEvent.getRevenueUsd(), 0);
        Assert.assertEquals("android",
                airlyticsPurchaseEvent.getPlatform());
        Assert.assertEquals("com.product.iap.renewing.1year.1",
                airlyticsPurchaseEvent.getProduct());
    }

    private String readTestPurchase(String path) throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(path)) {
            return IOUtils.toString(Objects.requireNonNull(inputStream), StandardCharsets.UTF_8);
        }
    }


    private PurchaseConsumer.PurchaseMetaData createMockedMetaDataFromNotification(Long time,
                                                                                   AndroidPurchaseConsumer.NotificationType notificationType) {
        PurchaseConsumer.PurchaseMetaData metaData = createMockedMetaDataFromAPP(time);
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(notificationType.name());
        return metaData;
    }

    private PurchaseConsumer.PurchaseMetaData createMockedMetaDataFromAPP(Long time) {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);
        metaData.setPurchaseToken(TEST_PURCHASE_TOKEN);
        metaData.setNotificationTime(time);
        metaData.setProductId("com.product.iap.renewing.1year.1");

        return metaData;
    }

    private SubscriptionPurchase createMockedSubscriptionPurchase(JSONObject subscriptionPurchaseHashRawObject) {
        SubscriptionPurchase subscriptionPurchase = PowerMockito.mock(SubscriptionPurchase.class);
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

        return subscriptionPurchase;
    }
}