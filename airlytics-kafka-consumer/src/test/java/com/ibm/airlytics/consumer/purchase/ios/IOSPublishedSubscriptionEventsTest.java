package com.ibm.airlytics.consumer.purchase.ios;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.purchase.AirlyticsInAppProduct;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;
import com.ibm.airlytics.consumer.purchase.PurchaseConsumer;
import com.ibm.airlytics.consumer.purchase.events.*;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class IOSPublishedSubscriptionEventsTest {

    private static final String TEST_DATE_FOLDER = "ios_consumer_test_data";
    private static final long maxSubscriptionEventAge = 31536000000L;


    @SuppressWarnings({"SpellCheckingInspection"})
    private static final String inAppProducts = "{\"com.product.iap.renewing.1year.ip1\":{\"priceUSDMicros\":9990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.ip1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1monthtrial.1\":{\"priceUSDMicros\":990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1monthtrial.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1month.1b\":{\"priceUSDMicros\":990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1month.1b\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.pro\":{\"priceUSDMicros\":29990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.pro\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.3months.1\":{\"priceUSDMicros\":9990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.3months.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1month.1\":{\"priceUSDMicros\":990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1month.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1month.pro\":{\"priceUSDMicros\":990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1month.pro\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.1\":{\"priceUSDMicros\":9990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.inapp.sub.1year.1\":{\"priceUSDMicros\":3990000,\"gracePeriod\":null,\"productId\":\"com.product.inapp.sub.1year.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.1c\":{\"priceUSDMicros\":39990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.1c\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.sub.renewing.1year.2\":{\"priceUSDMicros\":9990000,\"gracePeriod\":null,\"productId\":\"com.product.sub.renewing.1year.2\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.1b\":{\"priceUSDMicros\":19990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.1b\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null}}";
    private HashMap<String, AirlyticsInAppProduct> inAppAirlyticsProducts;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private String readTestReceipt(String path) throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(path)) {
            return IOUtils.toString(Objects.requireNonNull(inputStream), StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

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
    public void validateRemoveSubEventsDuplicationLogic() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchase_consumer_subscript_events_test1.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Map<String, BaseSubscriptionEvent> duplications = new HashMap<>();

            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {
                airlyticsPurchase.setUserId("");
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                try {
                    List<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            null, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    subscriptionEvents = purchaseConsumer.removeDuplications(subscriptionEvents, duplications);
                    Assert.assertEquals(0, purchaseConsumer.removeDuplications(subscriptionEvents, duplications).size());
                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }
            }

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

    }


    @Test
    public void subscriptionEventsFromNewPurchases() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchase_consumer_subscript_events_test1.json"),
                    IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();


            // make sure the last purchase is active to send
            airlyticsPurchases.get(airlyticsPurchases.size() - 1).setActive(true);
            airlyticsPurchases.get(airlyticsPurchases.size() - 2).setUpgradedToProductActive(true);

            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {
                airlyticsPurchase.setUserId("");
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                try {

                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            null, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    if (airlyticsPurchase.getProduct().equals("com.product.inapp.sub.1year.1")) {
                        Assert.assertEquals(2, subscriptionEvents.size());
                        // validate user-attribute-detected
                        SubscriptionPurchasedEvent subscriptionPurchasedEvent = (SubscriptionPurchasedEvent) subscriptionEvents.get(0);
                        SubscriptionExpiredEvent subscriptionExpiredEvent = (SubscriptionExpiredEvent) subscriptionEvents.get(1);
                    }
                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1year.1")) {
                        Assert.assertEquals(4, subscriptionEvents.size());
                        SubscriptionUpgradedEvent subscriptionUpgradedEvent = (SubscriptionUpgradedEvent) subscriptionEvents.get(2);
                        UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(3);

                        Assert.assertEquals((subscriptionUpgradedEvent.getAttributes()).getNewProductId(),
                                airlyticsPurchase.getUpgradedToProduct().getProductId());

                        Assert.assertEquals(userAttributeDetectedEvent.getEventTime(),
                                airlyticsPurchase.getUpgradedToProduct().getPurchaseDateMs().longValue());

                        Assert.assertEquals(false, (userAttributeDetectedEvent.getAttributes()).isPremiumTrial());

                        Assert.assertEquals((subscriptionUpgradedEvent.getAttributes()).getNewProductId(),
                                userAttributeDetectedEvent.getAttributes().premiumProductId);

                        Assert.assertEquals(29770221, subscriptionUpgradedEvent.getAttributes().getRevenueUsdMicros().longValue());
                    }
                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1year.pro")) {
                        Assert.assertEquals(0, subscriptionEvents.size());
                    }
                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }
            }

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void subscriptionEventsFromNewTrialPurchases() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "historical-renewal-after-trial-after.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();


            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {
                airlyticsPurchase.setUserId("");
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                try {

                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            null, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    if (airlyticsPurchase.getProduct().equals("com.product.inapp.sub.1year.1")) {
                        Assert.assertEquals(2, subscriptionEvents.size());
                        // validate user-attribute-detected
                        SubscriptionPurchasedEvent subscriptionPurchasedEvent = (SubscriptionPurchasedEvent) subscriptionEvents.get(0);
                        SubscriptionExpiredEvent subscriptionExpiredEvent = (SubscriptionExpiredEvent) subscriptionEvents.get(1);
                    }
                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1year.pro")) {
                        Assert.assertEquals(3, subscriptionEvents.size());
                        SubscriptionPurchasedEvent subscriptionPurchasedEvent = (SubscriptionPurchasedEvent) subscriptionEvents.get(0);
                        SubscriptionRenewedEvent subscriptionRenewedEvent = (SubscriptionRenewedEvent) subscriptionEvents.get(1);
                        UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(2);
                        Assert.assertEquals(airlyticsPurchase.getPeriodStartDate().longValue(), subscriptionRenewedEvent.getEventTime());
                        Assert.assertEquals(airlyticsPurchase.getStartDate().longValue(), subscriptionPurchasedEvent.getEventTime());
                        Assert.assertEquals(29.0, subscriptionPurchasedEvent.getAttributes().getRevenueUsdMicros().longValue()/1000000, 0.0);

                    }
                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }
            }

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void subscriptionTrialRevenueShouldBeZero() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "trial_before.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();


            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {
                airlyticsPurchase.setUserId("");
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                try {

                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            null, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1year.pro")) {
                        Assert.assertEquals(2, subscriptionEvents.size());
                        SubscriptionPurchasedEvent subscriptionPurchasedEvent = (SubscriptionPurchasedEvent) subscriptionEvents.get(0);
                        Assert.assertEquals(0.0, subscriptionPurchasedEvent.getAttributes().getRevenueUsdMicros().longValue(), 0.0);
                    }
                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }
            }

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void trialSubscriptionEventsFromNewPurchases() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "trial.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();


            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {
                airlyticsPurchase.setActive(true);
                airlyticsPurchase.setUserId("");
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                try {

                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            null, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);


                    Assert.assertEquals(2, subscriptionEvents.size());
                    SubscriptionPurchasedEvent subscriptionPurchasedEvent = (SubscriptionPurchasedEvent) subscriptionEvents.get(0);
                    UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(1);

                    Assert.assertEquals(true, (userAttributeDetectedEvent.getAttributes()).isPremiumTrial());


                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }
            }

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void subscriptionEventsFromUpgradedPurchases() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase upgradedSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgraded-not-cancelled.json"), IOSubscriptionPurchase.class);
            IOSubscriptionPurchase previousSubscriptionState = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgraded-not-cancelled-prior-to-upgrade.json"), IOSubscriptionPurchase.class);

            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    upgradedSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            List<AirlyticsPurchase> airlyticsPurchasesPreviousSubscriptionState = new IOSAirlyticsPurchaseState(
                    previousSubscriptionState,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            airlyticsPurchases.get(airlyticsPurchases.size() - 2).setUpgradedToProductActive(true);


            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {


                AirlyticsPurchase previousAirlyticsPurchase = airlyticsPurchasesPreviousSubscriptionState.stream().filter(airlyticsPurchase1 -> airlyticsPurchase1.getId().
                        equals(airlyticsPurchase.getId())).findFirst().orElse(null);
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                if (previousAirlyticsPurchase != null) {
                    previousAirlyticsPurchase.setUserId("dummy");
                }

                airlyticsPurchase.setUserId("dummy");

                try {
                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1year.1")) {
                        Assert.assertEquals(2, subscriptionEvents.size());
                        // validate user-attribute-detected
                        SubscriptionUpgradedEvent subscriptionUpgradedEvent = (SubscriptionUpgradedEvent) subscriptionEvents.get(0);
                        UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(1);

                        Assert.assertEquals(userAttributeDetectedEvent.getEventTime(),
                                airlyticsPurchase.getUpgradedToProduct().getPurchaseDateMs().longValue());


                        Assert.assertEquals(-258749, subscriptionUpgradedEvent.getAttributes().getRevenueUsdMicros().longValue());

                        Assert.assertEquals((subscriptionUpgradedEvent.getAttributes()).getNewProductId(),
                                userAttributeDetectedEvent.getAttributes().premiumProductId);
                    }
                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1month.pro")) {
                        Assert.assertEquals(0, subscriptionEvents.size());
                    }


                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }
            }

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void subscriptionEventsFromCanceledPurchases() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase upgradedSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "canceled_sub.json"), IOSubscriptionPurchase.class);
            IOSubscriptionPurchase previousSubscriptionState = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "not_canceled_sub.json"), IOSubscriptionPurchase.class);

            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    upgradedSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            List<AirlyticsPurchase> airlyticsPurchasesPreviousSubscriptionState = new IOSAirlyticsPurchaseState(
                    previousSubscriptionState,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();


            airlyticsPurchases.get(airlyticsPurchases.size() - 1).setActive(true);

            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {


                AirlyticsPurchase previousAirlyticsPurchase = airlyticsPurchasesPreviousSubscriptionState.stream().filter(airlyticsPurchase1 -> airlyticsPurchase1.getId().
                        equals(airlyticsPurchase.getId())).findFirst().orElse(null);
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                if (previousAirlyticsPurchase != null) {
                    previousAirlyticsPurchase.setUserId("dummy");
                }

                airlyticsPurchase.setUserId("dummy");

                try {
                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1year.1")) {
                        Assert.assertEquals(3, subscriptionEvents.size());
                        // validate user-attribute-detected
                        SubscriptionExpiredEvent subscriptionExpiredEvent = (SubscriptionExpiredEvent) subscriptionEvents.get(0);
                        SubscriptionCancelledEvent subscriptionCancelledEvent = (SubscriptionCancelledEvent) subscriptionEvents.get(1);
                        UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(2);


                        Assert.assertEquals(((SubscriptionExpiredEvent.SubscriptionExpiredAttributes) subscriptionExpiredEvent.getAttributes()).getSource(),
                                "cancellation");

                        Assert.assertEquals(userAttributeDetectedEvent.getEventTime(),
                                airlyticsPurchase.getCancellationDate().longValue());


                        Assert.assertEquals(((UserAttributeDetectedEvent.UserAttributeDetectedEventAttributes) userAttributeDetectedEvent.getAttributes()).getPremiumExpirationDate(),
                                subscriptionExpiredEvent.getEventTime());
                        Assert.assertEquals(subscriptionCancelledEvent.getEventTime(),
                                airlyticsPurchase.getCancellationDate().longValue());
                    }
                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1month.1")) {
                        Assert.assertEquals(0, subscriptionEvents.size());
                    }
                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1year.pro")) {
                        Assert.assertEquals(2, subscriptionEvents.size());
                        SubscriptionPurchasedEvent subscriptionPurchasedEvent = (SubscriptionPurchasedEvent) subscriptionEvents.get(0);
                        UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(1);
                    }

                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }
            }

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void subscriptionEventsFromRenewalPurchases() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase upgradedSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "new_renewal_for_subscription_event.json"), IOSubscriptionPurchase.class);
            IOSubscriptionPurchase previousSubscriptionState = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "renewal_for_subscription_event.json"), IOSubscriptionPurchase.class);

            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    upgradedSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            List<AirlyticsPurchase> airlyticsPurchasesPreviousSubscriptionState = new IOSAirlyticsPurchaseState(
                    previousSubscriptionState,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();


            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {


                AirlyticsPurchase previousAirlyticsPurchase = airlyticsPurchasesPreviousSubscriptionState.stream().filter(airlyticsPurchase1 -> airlyticsPurchase1.getId().
                        equals(airlyticsPurchase.getId())).findFirst().orElse(null);
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                if (previousAirlyticsPurchase != null) {
                    previousAirlyticsPurchase.setActive(true);
                    airlyticsPurchase.setActive(true);
                    previousAirlyticsPurchase.setUserId("dummy");
                }

                airlyticsPurchase.setUserId("dummy");

                try {
                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1year.1")) {
                        Assert.assertEquals(2, subscriptionEvents.size());
                        // validate event
                        SubscriptionRenewedEvent subscriptionRenewedEvent = (SubscriptionRenewedEvent) subscriptionEvents.get(0);
                        UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(1);


                        Assert.assertEquals(airlyticsPurchase.getExpirationDate().longValue(),
                                ((UserAttributeDetectedEvent.UserAttributeDetectedEventAttributes) userAttributeDetectedEvent.getAttributes()).getPremiumExpirationDate());
                        Assert.assertEquals(subscriptionRenewedEvent.getEventTime(), airlyticsPurchase.getPeriodStartDate().longValue());
                    }
                    if (airlyticsPurchase.getProduct().equals("com.product.inapp.sub.1year.1")) {
                        Assert.assertEquals(0, subscriptionEvents.size());
                    }

                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }
            }

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void subscriptionPurchasedAfterTrialPurchases() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase upgradedSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchased_after_trial.json"), IOSubscriptionPurchase.class);
            IOSubscriptionPurchase previousSubscriptionState = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "trial.json"), IOSubscriptionPurchase.class);

            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    upgradedSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            List<AirlyticsPurchase> airlyticsPurchasesPreviousSubscriptionState = new IOSAirlyticsPurchaseState(
                    previousSubscriptionState,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();


            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {


                AirlyticsPurchase previousAirlyticsPurchase = airlyticsPurchasesPreviousSubscriptionState.stream().filter(airlyticsPurchase1 -> airlyticsPurchase1.getId().
                        equals(airlyticsPurchase.getId())).findFirst().orElse(null);
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                if (previousAirlyticsPurchase != null) {
                    previousAirlyticsPurchase.setUserId("dummy");
                }

                try {
                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    SubscriptionRenewedEvent subscriptionRenewedEvent = (SubscriptionRenewedEvent) subscriptionEvents.get(0);
                    UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(1);

                    Assert.assertEquals(2, subscriptionEvents.size());
                    Assert.assertEquals(false, userAttributeDetectedEvent.getAttributes().premiumTrial);

                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }
            }

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void subscriptionCanceledAfterTrialPurchases() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase upgradedSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "trial_canceled.json"), IOSubscriptionPurchase.class);
            IOSubscriptionPurchase previousSubscriptionState = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "trial_before.json"), IOSubscriptionPurchase.class);

            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    upgradedSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            List<AirlyticsPurchase> airlyticsPurchasesPreviousSubscriptionState = new IOSAirlyticsPurchaseState(
                    previousSubscriptionState,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();


            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {


                AirlyticsPurchase previousAirlyticsPurchase = airlyticsPurchasesPreviousSubscriptionState.stream().filter(airlyticsPurchase1 -> airlyticsPurchase1.getId().
                        equals(airlyticsPurchase.getId())).findFirst().orElse(null);
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                if (previousAirlyticsPurchase != null) {
                    previousAirlyticsPurchase.setUserId("dummy");
                }

                try {
                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    // validate event
                    SubscriptionExpiredEvent subscriptionExpiredEvent = (SubscriptionExpiredEvent) subscriptionEvents.get(0);
                    SubscriptionCancelledEvent subscriptionCancelledEvent = (SubscriptionCancelledEvent) subscriptionEvents.get(1);
                    UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(2);

                    Assert.assertEquals(3, subscriptionEvents.size());
                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }
            }

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void subscriptionDoubleExpirationPurchases() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase upgradedSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "double-sub-expired-event-after.json"), IOSubscriptionPurchase.class);
            IOSubscriptionPurchase previousSubscriptionState = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "double-sub-expired-event-before.json"), IOSubscriptionPurchase.class);

            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    upgradedSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            List<AirlyticsPurchase> airlyticsPurchasesPreviousSubscriptionState = new IOSAirlyticsPurchaseState(
                    previousSubscriptionState,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {


                AirlyticsPurchase previousAirlyticsPurchase = airlyticsPurchasesPreviousSubscriptionState.stream().filter(airlyticsPurchase1 -> airlyticsPurchase1.getId().
                        equals(airlyticsPurchase.getId())).findFirst().orElse(null);
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                if (previousAirlyticsPurchase != null) {
                    previousAirlyticsPurchase.setUserId("dummy");
                    previousAirlyticsPurchase.setActive(true);
                }

                if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1year.1")) {
                    try {
                        ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                                previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                                null, "ios", maxSubscriptionEventAge);

                        Assert.assertEquals(0, subscriptionEvents.size());
                    } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                        illegalSubscriptionEventArguments.printStackTrace();
                    }
                }

                if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1month.pro")) {
                    try {
                        ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                                previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                                null, "ios", maxSubscriptionEventAge);

                        Assert.assertEquals(2, subscriptionEvents.size());
                    } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                        illegalSubscriptionEventArguments.printStackTrace();
                    }
                }
            }

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void subscriptionDisableTrialPurchasesTest() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase upgradedSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "trial-after-auto-trial-disable.json"), IOSubscriptionPurchase.class);
            IOSubscriptionPurchase previousSubscriptionState = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "trial-before-auto-trial-disable.json"), IOSubscriptionPurchase.class);

            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    upgradedSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            List<AirlyticsPurchase> airlyticsPurchasesPreviousSubscriptionState = new IOSAirlyticsPurchaseState(
                    previousSubscriptionState,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {


                AirlyticsPurchase previousAirlyticsPurchase = airlyticsPurchasesPreviousSubscriptionState.stream().filter(airlyticsPurchase1 -> airlyticsPurchase1.getId().
                        equals(airlyticsPurchase.getId())).findFirst().orElse(null);
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                if (previousAirlyticsPurchase != null) {
                    previousAirlyticsPurchase.setUserId("dummy");
                    previousAirlyticsPurchase.setActive(true);
                    airlyticsPurchase.setActive(true);
                    airlyticsPurchase.setAutoRenewStatusChangeDate(System.currentTimeMillis());
                }


                try {
                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    Assert.assertEquals(2, subscriptionEvents.size());


                    SubscriptionRenewalStatusChangedSubscriptionEvent subscriptionRenewalStatusChangedSubscriptionEvent =
                            (SubscriptionRenewalStatusChangedSubscriptionEvent) subscriptionEvents.get(0);
                    UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(1);


                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }


            }

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void subscriptionEventsFromExpiredPurchases() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase upgradedSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "expired_sub.json"), IOSubscriptionPurchase.class);
            IOSubscriptionPurchase previousSubscriptionState = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "not_expired_sub.json"), IOSubscriptionPurchase.class);

            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    upgradedSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            List<AirlyticsPurchase> airlyticsPurchasesPreviousSubscriptionState = new IOSAirlyticsPurchaseState(
                    previousSubscriptionState,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();


            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {


                AirlyticsPurchase previousAirlyticsPurchase = airlyticsPurchasesPreviousSubscriptionState.stream().filter(airlyticsPurchase1 -> airlyticsPurchase1.getId().
                        equals(airlyticsPurchase.getId())).findFirst().orElse(null);
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                if (previousAirlyticsPurchase != null) {
                    previousAirlyticsPurchase.setUserId("dummy");
                }

                airlyticsPurchase.setUserId("dummy");

                try {
                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1year.pro")) {
                        Assert.assertEquals(2, subscriptionEvents.size());
                        // validate event
                        SubscriptionExpiredEvent subscriptionExpiredEvent = (SubscriptionExpiredEvent) subscriptionEvents.get(0);
                        UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(1);


                        Assert.assertNull(subscriptionExpiredEvent.getAttributes().getSource());

                        Assert.assertEquals(userAttributeDetectedEvent.getEventTime(),
                                airlyticsPurchase.getExpirationDate().longValue());


                        Assert.assertEquals(airlyticsPurchase.getExpirationDate().longValue(),
                                ((UserAttributeDetectedEvent.UserAttributeDetectedEventAttributes) userAttributeDetectedEvent.getAttributes()).getPremiumExpirationDate());
                    }
                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1month.1")) {
                        Assert.assertEquals(0, subscriptionEvents.size());
                    }
                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1year.1")) {
                        Assert.assertEquals(0, subscriptionEvents.size());
                    }


                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }
            }
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void subscriptionEventsFromRenewalStateChanged() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase upgradedSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "disable_renewable.json"), IOSubscriptionPurchase.class);
            IOSubscriptionPurchase previousSubscriptionState = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "active_renewable.json"), IOSubscriptionPurchase.class);

            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    upgradedSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            List<AirlyticsPurchase> airlyticsPurchasesPreviousSubscriptionState = new IOSAirlyticsPurchaseState(
                    previousSubscriptionState,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();


            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {


                AirlyticsPurchase previousAirlyticsPurchase = airlyticsPurchasesPreviousSubscriptionState.stream().filter(airlyticsPurchase1 -> airlyticsPurchase1.getId().
                        equals(airlyticsPurchase.getId())).findFirst().orElse(null);
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                if (previousAirlyticsPurchase != null) {
                    previousAirlyticsPurchase.setActive(true);
                    airlyticsPurchase.setActive(true);
                    previousAirlyticsPurchase.setUserId("dummy");
                }

                airlyticsPurchase.setUserId("dummy");


                try {
                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1month.pro")) {
                        Assert.assertEquals(0, subscriptionEvents.size());
                    }

                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }


                long autoRenewStatusChangeDate = System.currentTimeMillis();
                airlyticsPurchase.setAutoRenewStatusChangeDate(autoRenewStatusChangeDate);

                try {
                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1month.pro")) {
                        Assert.assertEquals(2, subscriptionEvents.size());
                        // validate event
                        SubscriptionRenewalStatusChangedSubscriptionEvent subscriptionRenewalStatusChangedSubscriptionEvent = (SubscriptionRenewalStatusChangedSubscriptionEvent) subscriptionEvents.get(0);
                        UserAttributeDetectedEvent userAttributeDetectedEvent = (UserAttributeDetectedEvent) subscriptionEvents.get(1);


                        Assert.assertFalse(((UserAttributeDetectedEvent.UserAttributeDetectedEventAttributes)
                                userAttributeDetectedEvent.getAttributes()).premiumAutoRenewStatus);
                        Assert.assertTrue(((UserAttributeDetectedEvent.UserAttributeDetectedEventAttributes)
                                userAttributeDetectedEvent.getPreviousValues()).premiumAutoRenewStatus);

                        Assert.assertEquals(userAttributeDetectedEvent.getEventTime(),
                                autoRenewStatusChangeDate);


                        Assert.assertEquals(((UserAttributeDetectedEvent.UserAttributeDetectedEventAttributes) userAttributeDetectedEvent.getAttributes()).isPremiumAutoRenewStatus(),
                                airlyticsPurchase.getAutoRenewStatus());
                        Assert.assertEquals(((SubscriptionRenewalStatusChangedSubscriptionEvent.
                                        SubscriptionRenewalStatusChangeAttributes) subscriptionRenewalStatusChangedSubscriptionEvent.
                                        getAttributes()).getPremiumProductId(),
                                airlyticsPurchase.getProduct());
                    }

                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }

                try {
                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1month.pro")) {
                        Assert.assertEquals(2, subscriptionEvents.size());
                        // validate event
                        SubscriptionRenewalStatusChangedSubscriptionEvent subscriptionRenewalStatusChangedSubscriptionEvent = (SubscriptionRenewalStatusChangedSubscriptionEvent) subscriptionEvents.get(0);
                        Assert.assertEquals(((SubscriptionRenewalStatusChangedSubscriptionEvent.
                                        SubscriptionRenewalStatusChangeAttributes) subscriptionRenewalStatusChangedSubscriptionEvent.
                                        getAttributes()).getPremiumProductId(),
                                airlyticsPurchase.getProduct());
                    }

                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }
            }
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void subscriptionEventsFromRenewalStateChanged2() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase upgradedSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "renew-sub-event-bug-after.json"), IOSubscriptionPurchase.class);
            IOSubscriptionPurchase previousSubscriptionState = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "renew-sub-event-bug-before.json"), IOSubscriptionPurchase.class);

            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    upgradedSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            List<AirlyticsPurchase> airlyticsPurchasesPreviousSubscriptionState = new IOSAirlyticsPurchaseState(
                    previousSubscriptionState,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();


            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {

                AirlyticsPurchase previousAirlyticsPurchase = airlyticsPurchasesPreviousSubscriptionState.stream().filter(airlyticsPurchase1 -> airlyticsPurchase1.getId().
                        equals(airlyticsPurchase.getId())).findFirst().orElse(null);
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                if (previousAirlyticsPurchase != null) {
                    previousAirlyticsPurchase.setActive(true);
                    airlyticsPurchase.setActive(true);
                    previousAirlyticsPurchase.setUserId("dummy");
                }

                airlyticsPurchase.setUserId("dummy");

                try {
                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1month.pro")) {
                        Assert.assertEquals(2, subscriptionEvents.size());
                    }

                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }

            }
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void subscriptionEventsFromRepeatingPurchase() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase upgradedSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "same-sub-purchase-after-a-while.json"), IOSubscriptionPurchase.class);
            IOSubscriptionPurchase previousSubscriptionState = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "same-sub-purchase-after-a-while-before.json"), IOSubscriptionPurchase.class);

            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    upgradedSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            List<AirlyticsPurchase> airlyticsPurchasesPreviousSubscriptionState = new IOSAirlyticsPurchaseState(
                    previousSubscriptionState,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();


            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {

                AirlyticsPurchase previousAirlyticsPurchase = airlyticsPurchasesPreviousSubscriptionState.stream().filter(airlyticsPurchase1 -> airlyticsPurchase1.getId().
                        equals(airlyticsPurchase.getId())).findFirst().orElse(null);
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                if (previousAirlyticsPurchase != null) {
                    previousAirlyticsPurchase.setActive(true);
                    airlyticsPurchase.setActive(true);
                    previousAirlyticsPurchase.setUserId("dummy");
                }

                airlyticsPurchase.setUserId("dummy");

                try {
                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1month.pro") &&
                            subscriptionEvents.size() > 0) {
                        Assert.assertEquals(2, subscriptionEvents.size());
                        Assert.assertEquals(1611129593000L, subscriptionEvents.get(0).getEventTime());
                    }

                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }

            }
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void subscriptionRenewalEvents() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase upgradedSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "renewal_bug_after.json"), IOSubscriptionPurchase.class);
            IOSubscriptionPurchase previousSubscriptionState = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "renewal_bug_before.json"), IOSubscriptionPurchase.class);

            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    upgradedSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            List<AirlyticsPurchase> airlyticsPurchasesPreviousSubscriptionState = new IOSAirlyticsPurchaseState(
                    previousSubscriptionState,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();


            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {

                AirlyticsPurchase previousAirlyticsPurchase = airlyticsPurchasesPreviousSubscriptionState.stream().filter(airlyticsPurchase1 -> airlyticsPurchase1.getId().
                        equals(airlyticsPurchase.getId())).findFirst().orElse(null);
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                if (previousAirlyticsPurchase != null) {
                    previousAirlyticsPurchase.setActive(true);
                    airlyticsPurchase.setActive(true);
                    previousAirlyticsPurchase.setUserId("dummy");
                }

                airlyticsPurchase.setUserId("dummy");

                try {
                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1month.1")) {
                        Assert.assertEquals(2, subscriptionEvents.size());
                        Assert.assertEquals(1611129362000L, subscriptionEvents.get(0).getEventTime());
                    }

                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }

            }
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void subscriptionNewPurchaseBug() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase upgradedSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "new_purchase_sub_event_bug_after.json"), IOSubscriptionPurchase.class);
            IOSubscriptionPurchase previousSubscriptionState = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "new_purchase_sub_event_bug_before.json"), IOSubscriptionPurchase.class);

            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    upgradedSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            List<AirlyticsPurchase> airlyticsPurchasesPreviousSubscriptionState = new IOSAirlyticsPurchaseState(
                    previousSubscriptionState,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {

                AirlyticsPurchase previousAirlyticsPurchase = airlyticsPurchasesPreviousSubscriptionState.stream().filter(airlyticsPurchase1 -> airlyticsPurchase1.getId().
                        equals(airlyticsPurchase.getId())).findFirst().orElse(null);
                airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

                airlyticsPurchase.setUserId("");

                try {
                    ArrayList<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                            previousAirlyticsPurchase, "", "", inAppAirlyticsProducts,
                            null, "ios", maxSubscriptionEventAge);

                    if (airlyticsPurchase.getProduct().equals("com.product.iap.renewing.1year.1")) {
                        Assert.assertEquals(1611332621000L, subscriptionEvents.get(0).getEventTime());
                    }

                } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                    illegalSubscriptionEventArguments.printStackTrace();
                }

            }
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

}
