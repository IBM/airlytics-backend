package com.ibm.airlytics.consumer.purchase.ios;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.purchase.AirlyticsInAppProduct;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;
import com.ibm.airlytics.consumer.purchase.PurchaseConsumer;
import com.ibm.airlytics.utilities.Duration;
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


public class IOSAirlyticsPurchaseStateTest {

    private static final String TEST_DATE_FOLDER = "ios_consumer_test_data";

    @SuppressWarnings({"SpellCheckingInspection"})
    private static final String inAppProducts = "{\"com.product.iap.renewing.1year.ip1\":{\"priceUSDMicros\":9990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.ip1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1monthtrial.1\":{\"priceUSDMicros\":990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1monthtrial.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1month.1b\":{\"priceUSDMicros\":990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1month.1b\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.pro\":{\"priceUSDMicros\":29990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.pro\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.3months.1\":{\"priceUSDMicros\":9990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.3months.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1month.1\":{\"priceUSDMicros\":990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1month.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1month.pro\":{\"priceUSDMicros\":990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1month.pro\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1M\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.1\":{\"priceUSDMicros\":9990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.inapp.sub.1year.1\":{\"priceUSDMicros\":3990000,\"gracePeriod\":null,\"productId\":\"com.product.inapp.sub.1year.1\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":false,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.1c\":{\"priceUSDMicros\":39990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.1c\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.sub.renewing.1year.2\":{\"priceUSDMicros\":9990000,\"gracePeriod\":null,\"productId\":\"com.product.sub.renewing.1year.2\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null},\"com.product.iap.renewing.1year.1b\":{\"priceUSDMicros\":19990000,\"gracePeriod\":null,\"productId\":\"com.product.iap.renewing.1year.1b\",\"trialPeriod\":null,\"subscriptionPeriod\":\"P1Y\",\"autoRenewing\":true,\"introPriceUSDMicros\":null}}";
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
    public void testCreateIOSubscriptionPurchase() {

        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "monthly_receipt.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 1);
            Assert.assertEquals(airlyticsPurchases.get(0).getPeriods().intValue(), 9);
            Assert.assertEquals(airlyticsPurchases.get(0).getRevenueUsd().intValue(), 8910000);
            Assert.assertEquals(airlyticsPurchases.get(0).getId(), "460000195957416");
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testCreateDevIOSubscriptionPurchase() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "dev_receipt.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(152, airlyticsPurchases.size());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testAutoRenewStatus() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "auto_renew_status.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(1, airlyticsPurchases.size());
            Assert.assertEquals(true, airlyticsPurchases.get(0).getAutoRenewStatus());
            Assert.assertEquals(airlyticsPurchases.get(0).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());

            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {
                if (airlyticsPurchase.getStartDate() > airlyticsPurchase.getExpirationDate()) {
                    Assert.fail();
                }
            }
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNotRenewalExpiredShouldBeNotActive() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "non_renewal.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(2, airlyticsPurchases.size());
            Assert.assertEquals(true, airlyticsPurchases.get(1).getAutoRenewStatus());
            Assert.assertEquals(false, airlyticsPurchases.get(1).getActive());
            Assert.assertEquals(false, airlyticsPurchases.get(0).getActive());
            Assert.assertEquals(airlyticsPurchases.get(0).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());
            Assert.assertEquals(airlyticsPurchases.get(1).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNotRenewalExpirationDateShouldBeOnePeriod() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "new_renewal_plus_updated.json"), IOSubscriptionPurchase.class);
            ioSubscriptionPurchase.getLatestReceiptInfo().get(0).setExpiresDateMs(System.currentTimeMillis() + 10000);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            long periodInMilliseconds = Duration.durationToMilliSeconds(inAppAirlyticsProducts.get(airlyticsPurchases.get(0).
                    getProduct()).getSubscriptionPeriod());
            Assert.assertEquals(3, airlyticsPurchases.size());
            Assert.assertEquals(airlyticsPurchases.get(0).getStartDate() + periodInMilliseconds, (long) airlyticsPurchases.get(0).getExpirationDate());
            Assert.assertEquals(false, airlyticsPurchases.get(0).getActive());

            Assert.assertEquals(airlyticsPurchases.get(0).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());
            Assert.assertEquals(airlyticsPurchases.get(1).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.UPGRADED.getStatus());
            Assert.assertEquals(airlyticsPurchases.get(2).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.ACTIVE.getStatus());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testUpgradingStartDateShouldBeEqualActualEndDateUpgraded() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "new_renewal_plus_updated.json"), IOSubscriptionPurchase.class);
            ioSubscriptionPurchase.getLatestReceiptInfo().get(0).setExpiresDateMs(System.currentTimeMillis() + 10000);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(3, airlyticsPurchases.size());
            Assert.assertTrue(airlyticsPurchases.get(2).getStartDate() - airlyticsPurchases.get(1).getActualEndDate() < 2000);
            Assert.assertEquals(true, airlyticsPurchases.get(2).getActive());
            Assert.assertEquals(false, airlyticsPurchases.get(1).getActive());

            Assert.assertEquals(airlyticsPurchases.get(0).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());
            Assert.assertEquals(airlyticsPurchases.get(1).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.UPGRADED.getStatus());
            Assert.assertEquals(airlyticsPurchases.get(2).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.ACTIVE.getStatus());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testRelativePeriodAndRevenueOnUpgrade() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "new_renewal_plus_updated.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(3, airlyticsPurchases.size());
            Assert.assertEquals(new Double(1), airlyticsPurchases.get(0).getPeriods());
            Assert.assertEquals(new Double(1.047), airlyticsPurchases.get(1).getPeriods());
            Assert.assertEquals(new Double(1), airlyticsPurchases.get(2).getPeriods());
            Assert.assertEquals(Duration.Periodicity.YEARLY.name(), airlyticsPurchases.get(2).getDuration());
            Assert.assertEquals(IOSPurchaseConsumer.CancellationReason.SUBSCRIPTION_UPGRADED, airlyticsPurchases.get(1).getCancellationReason());
            Assert.assertEquals((9990000 + 469530), airlyticsPurchases.get(1).getRevenueUsd().longValue());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testYearlyUpgradedPeriodShouldBeSmallerThenOne() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "yearly_upgraded_in_the_first_period.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(2, airlyticsPurchases.size());
            Assert.assertTrue(airlyticsPurchases.get(0).getPeriods() < 1);
            Assert.assertTrue(airlyticsPurchases.get(0).getUpgraded());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNonRenewalOf20YearsShouldBeActive() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "huge_not_renewal_receipt.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(false, airlyticsPurchases.get(0).getActive());
            Assert.assertEquals(34, airlyticsPurchases.size());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testThreeNonRenewalShouldNotBeActive() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "three_non_renewal_active.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(3, airlyticsPurchases.size());
            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {
                Assert.assertEquals(1, airlyticsPurchase.getPeriods().intValue());
            }
            Assert.assertEquals(false, airlyticsPurchases.get(0).getActive());


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testInteractiveRenewalShouldModifyLastPaymentAction() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);
        metaData.setNotificationType(IOSPurchaseConsumer.NotificationType.INTERACTIVE_RENEWAL.name());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "interactive_renewal_bug.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(3, airlyticsPurchases.size());
            Assert.assertEquals(1628917673000L, airlyticsPurchases.get(2).getLastPaymentActionDate(), 0);
            Assert.assertEquals(1574344873000L, airlyticsPurchases.get(1).getLastPaymentActionDate(), 0);
            Assert.assertEquals(1574344873000L, airlyticsPurchases.get(0).getLastPaymentActionDate(), 0);


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testMonthlyShouldBeUpgraded() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgrade_bug_fixing.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(5, airlyticsPurchases.size());
            Assert.assertEquals(true, airlyticsPurchases.get(3).getUpgraded());
            Assert.assertEquals(airlyticsPurchases.get(3).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.UPGRADED.getStatus());
            Assert.assertEquals(airlyticsPurchases.get(0).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());
            Assert.assertEquals(airlyticsPurchases.get(1).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());
            Assert.assertEquals(airlyticsPurchases.get(2).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());

            Assert.assertEquals(airlyticsPurchases.get(3).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.UPGRADED.getStatus());
            Assert.assertEquals(airlyticsPurchases.get(4).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testExpirationReasonShouldBeCanceled() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "expiration_reason_cancel.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(1, airlyticsPurchases.size());
            Assert.assertEquals(IOSPurchaseConsumer.ExpirationReason.getNameByType(1), airlyticsPurchases.get(0).getExpirationReason());
            Assert.assertEquals(IOSPurchaseConsumer.CancellationReason.getNameByType(0, false),
                    airlyticsPurchases.get(0).getCancellationReason());
            Assert.assertEquals(airlyticsPurchases.get(0).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.CANCELED.getStatus());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNonRenewableExpirationDateShouldIgnoreActual() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "non_renewable_expiration_date.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            long periodInMilliseconds = Duration.durationToMilliSeconds("P1Y");

            Assert.assertEquals(2, airlyticsPurchases.size());
            Assert.assertEquals((airlyticsPurchases.get(0).getStartDate() + periodInMilliseconds), (long) airlyticsPurchases.get(0).getExpirationDate());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void upgradedSubscriptionShouldHaveAtLessTwoReceipts() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgrade_receipt.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(2, airlyticsPurchases.size());
            Assert.assertEquals(airlyticsPurchases.get(1).getProduct(), airlyticsPurchases.get(0).getUpgradedToProduct().getProductId());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void calculateRevenueWithIntroPrice() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "intro-price-available.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(5, airlyticsPurchases.size());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void calculateActiveStatusTest() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "fix_active_status_bug.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(2, airlyticsPurchases.size());
            Assert.assertEquals(false, airlyticsPurchases.get(1).getActive());
            Assert.assertEquals(false, airlyticsPurchases.get(0).getActive());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void twoActivePurchaseShouldBeActiveTest() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "two-active-products.json"), IOSubscriptionPurchase.class);
            ioSubscriptionPurchase.getLatestReceiptInfo().get(0).setExpiresDateMs(System.currentTimeMillis() + 10000);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(3, airlyticsPurchases.size());
            Assert.assertEquals(false, airlyticsPurchases.get(1).getActive());
            Assert.assertEquals(true, airlyticsPurchases.get(2).getActive());
            Assert.assertEquals(false, airlyticsPurchases.get(1).getAutoRenewStatus());
            Assert.assertEquals(true, airlyticsPurchases.get(2).getAutoRenewStatus());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void activePurchaseShouldBeRenewableTest() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "active_renewable.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(1, airlyticsPurchases.size());
            Assert.assertEquals(true, airlyticsPurchases.get(0).getAutoRenewStatus());
            Assert.assertEquals(3, airlyticsPurchases.get(0).getRenewalsHistory().size());


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void lastPaymentColumnTest() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "non-renewal-for-last-payment-column-test.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(6, airlyticsPurchases.size());
            Assert.assertEquals(airlyticsPurchases.get(0).getLastPaymentActionDate(), airlyticsPurchases.get(0).getStartDate());
            Assert.assertEquals(airlyticsPurchases.get(1).getLastPaymentActionDate(), airlyticsPurchases.get(1).getStartDate());
            Assert.assertEquals(airlyticsPurchases.get(2).getLastPaymentActionDate(), airlyticsPurchases.get(2).getStartDate());
            Assert.assertTrue(airlyticsPurchases.get(0).getStartDate() < airlyticsPurchases.get(0).getExpirationDate());
            Assert.assertTrue(airlyticsPurchases.get(1).getStartDate() > airlyticsPurchases.get(0).getExpirationDate());
            Assert.assertTrue(airlyticsPurchases.get(2).getStartDate() > airlyticsPurchases.get(1).getExpirationDate());
            Assert.assertTrue(airlyticsPurchases.get(2).getStartDate() < airlyticsPurchases.get(2).getExpirationDate());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void lastPaymentColumnTest2() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "non-renewal-for-last-payment-column-test2.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(3, airlyticsPurchases.size());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void revenueShouldBePositive() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "negative-revenue.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(2, airlyticsPurchases.size());
            Assert.assertTrue(airlyticsPurchases.get(0).getRevenueUsd() >= 0);
            Assert.assertTrue(airlyticsPurchases.get(1).getRevenueUsd() >= 0);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void revenueShouldBePositive2() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "negative-revenue2.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(12, airlyticsPurchases.size());
            for (AirlyticsPurchase airlyticsPurchase : airlyticsPurchases) {
                Assert.assertTrue(airlyticsPurchase.getRevenueUsd() >= 0);
            }

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void refundedTransactionPeriodShouldBeZero() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "refunded-transaction-renewable.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(2, airlyticsPurchases.size());
            Assert.assertEquals(2, airlyticsPurchases.get(0).getPeriods().intValue());


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void upgradedToTheSameProductCancellationReasonShouldBeReset() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgraded-to-the-same-product.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(2, airlyticsPurchases.size());
            Assert.assertNull(airlyticsPurchases.get(1).getCancellationReason());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void purchasedStartDateShouldBeOfPurchasedProduct() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchased-after-trial.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(1, airlyticsPurchases.size());
            Assert.assertEquals(1603372253000L, airlyticsPurchases.get(0).getStartDate().longValue());
            Assert.assertEquals(1603631453000L, airlyticsPurchases.get(0).getPeriodStartDate().longValue());
            Assert.assertEquals(false, airlyticsPurchases.get(0).getTrial());
            Assert.assertEquals(1603372253000L, airlyticsPurchases.get(0).getLastPaymentActionDate().longValue());
            Assert.assertEquals(1603372253000L, airlyticsPurchases.get(0).getTrialStartDate().longValue());
            Assert.assertEquals(1603631453000L, airlyticsPurchases.get(0).getTrialEndDate().longValue());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void upgradedIsNotCancelledBug() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "updated-purchase-bug.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(3, airlyticsPurchases.size());
            Assert.assertNotNull(airlyticsPurchases.get(0).getCancellationDate());
            Assert.assertEquals(airlyticsPurchases.get(1).getStartDate().longValue(), airlyticsPurchases.get(0).getCancellationDate().longValue());
            Assert.assertEquals(airlyticsPurchases.get(1).getStartDate().longValue(), airlyticsPurchases.get(0).getActualEndDate().longValue());

            Assert.assertEquals(8041950, airlyticsPurchases.get(0).getRevenueUsd().longValue());
            Assert.assertEquals(39990000, airlyticsPurchases.get(1).getRevenueUsd().longValue());
            Assert.assertEquals(29990000, airlyticsPurchases.get(2).getRevenueUsd().longValue());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void upgradedOnRenewalShouldBeCancelled() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgraded-on-renewal-is-cancelled.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(2, airlyticsPurchases.size());
            Assert.assertEquals(1, airlyticsPurchases.get(0).getPeriods().intValue());
            Assert.assertEquals(airlyticsPurchases.get(0).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.UPGRADED.getStatus());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void upgradedOnRenewalShouldBeCancelled2() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgraded-on-renewal-is-cancelled2.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(3, airlyticsPurchases.size());
            Assert.assertEquals(1, airlyticsPurchases.get(1).getPeriods().intValue());
            Assert.assertNotNull(airlyticsPurchases.get(1).getCancellationDate());
            Assert.assertEquals(airlyticsPurchases.get(1).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.UPGRADED.getStatus());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void upgradedOnRenewalShouldBeCancelled3() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgraded-on-renewal-is-cancelled3.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(4, airlyticsPurchases.size());
            Assert.assertEquals(2, airlyticsPurchases.get(2).getPeriods().intValue());
            Assert.assertNotNull(airlyticsPurchases.get(2).getCancellationDate());
            Assert.assertEquals(airlyticsPurchases.get(2).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.UPGRADED.getStatus());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void renewedAfterTrialPeriodRevenueTest() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchased_after_trial.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(1, airlyticsPurchases.size());
            Assert.assertNotNull(airlyticsPurchases.get(0).getTrialEndDate());
            Assert.assertEquals(29.0, airlyticsPurchases.get(0).getRevenueUsd() / 1000000, 0L);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void upgradedTrialPeriodRevenueTestShouldBeZero() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchased_after_trial.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(1, airlyticsPurchases.size());
            Assert.assertNotNull(airlyticsPurchases.get(0).getTrialEndDate());
            Assert.assertEquals(29.0, airlyticsPurchases.get(0).getRevenueUsd() / 1000000, 0L);
            Assert.assertEquals(1, airlyticsPurchases.get(0).getPeriods().doubleValue(), 0.0);

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void trialTransactionShouldHaveStartEndDate() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "currently_trial.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(1, airlyticsPurchases.size());
            Assert.assertNotNull(airlyticsPurchases.get(0).getTrialEndDate());
            Assert.assertNotNull(airlyticsPurchases.get(0).getTrialStartDate());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void upgradedTrialShouldHaveAlwaysZeroRevenue() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "trial_upgraded.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(3, airlyticsPurchases.size());
            Assert.assertNotNull(airlyticsPurchases.get(1).getRevenueUsd());
            Assert.assertEquals(0, airlyticsPurchases.get(1).getPeriods().doubleValue(), 0.0);
            Assert.assertEquals(0, airlyticsPurchases.get(1).getRevenueUsd(), 0.0);
            Assert.assertEquals(1, airlyticsPurchases.get(2).getPeriods().doubleValue(), 0.0);
            Assert.assertEquals(9990000, airlyticsPurchases.get(2).getRevenueUsd(), 0.0);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void receiptInGracePeriodTest() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "in_grace_period.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(1, airlyticsPurchases.size());
            Assert.assertFalse(airlyticsPurchases.get(0).getGrace());
            Assert.assertEquals(PurchaseConsumer.SubscriptionStatus.EXPIRED.name(), airlyticsPurchases.get(0).getSubscriptionStatus());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void trialTransactionShouldHaveAlwaysZeroRevenue() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "long_offer_code_trial.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(1, airlyticsPurchases.size());
            Assert.assertNotNull(airlyticsPurchases.get(0).getRevenueUsd());
            Assert.assertEquals(0, airlyticsPurchases.get(0).getPeriods().doubleValue(), 0.0);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void trialTransactionShouldHaveStartEndDate2() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "currently_trial2.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(3, airlyticsPurchases.size());
            Assert.assertNotNull(airlyticsPurchases.get(2).getTrialEndDate());
            Assert.assertNotNull(airlyticsPurchases.get(2).getTrialStartDate());
            Assert.assertEquals(airlyticsPurchases.get(2).getStartDate(), airlyticsPurchases.get(2).getTrialStartDate());
            Assert.assertEquals(airlyticsPurchases.get(2).getExpirationDate(), airlyticsPurchases.get(2).getTrialEndDate());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void trialTransactionShouldHaveStartEndDate3() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "currently_trial3.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(3, airlyticsPurchases.size());
            Assert.assertNotNull(airlyticsPurchases.get(2).getTrialEndDate());
            Assert.assertNotNull(airlyticsPurchases.get(2).getTrialStartDate());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void multiUpgradesShouldHaveCancellationDates() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "multi-upgrades.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(6, airlyticsPurchases.size());
            airlyticsPurchases.stream().filter(airlyticsPurchase -> airlyticsPurchase.getUpgraded()).forEach(airlyticsPurchase -> {
                Assert.assertNotNull(airlyticsPurchase.getCancellationDate());
                Assert.assertEquals(airlyticsPurchase.getSubscriptionStatus(),
                        PurchaseConsumer.SubscriptionStatus.UPGRADED.getStatus());
            });

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void toUpgradedProductTest() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "to_upgraded_product.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.get(3).getProduct(), airlyticsPurchases.get(2).getUpgradedToProduct().getProductId());
            Assert.assertEquals(4, airlyticsPurchases.size());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void upgradedShouldBeCancelledProductTest() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgraded-not-cancelled.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.get(0).getUpgraded(), true);
            Assert.assertNotNull(airlyticsPurchases.get(0).getCancellationDate());
            Assert.assertEquals(airlyticsPurchases.get(0).getUpgradedToProduct().getProductId(),
                    airlyticsPurchases.get(1).getProduct());
            Assert.assertEquals(airlyticsPurchases.get(1).getUpgradedFromProduct().getProductId(),
                    airlyticsPurchases.get(0).getProduct());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void upgradedShouldBeCancelledProductTest1() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgraded-not-cancelled-bug.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 3);

            airlyticsPurchases.stream().filter(airlyticsPurchase -> airlyticsPurchase.getUpgraded()).
                    forEach(airlyticsPurchase -> {
                        Assert.assertNotNull(airlyticsPurchase.getCancellationDate());
                    });

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void upgradedShouldBeCancelledProductTest2() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "upgraded-not-cancelled-bug2.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 3);
            airlyticsPurchases.stream().filter(airlyticsPurchase -> airlyticsPurchase.getUpgraded()).
                    forEach(airlyticsPurchase -> {
                        Assert.assertNotNull(airlyticsPurchase.getCancellationDate());
                    });
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
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "same-sub-purchase-after-a-while.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 2);
            airlyticsPurchases.stream().filter(airlyticsPurchase -> airlyticsPurchase.getUpgraded()).
                    forEach(airlyticsPurchase -> {
                        Assert.assertEquals(2.0, airlyticsPurchase.getPeriods().doubleValue());
                    });
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void sub1year1PurchaseShouldNotBeActive() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "com.product.inapp.sub.1year.1.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 1);
            Assert.assertFalse(airlyticsPurchases.get(0).getActive());
            Assert.assertEquals(airlyticsPurchases.get(0).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void old_1month_PurchaseShouldNotBeActive() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "old-1month.1-is-still-active.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 3);
            Assert.assertFalse(airlyticsPurchases.get(0).getActive());
            Assert.assertEquals(airlyticsPurchases.get(0).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());
            Assert.assertFalse(airlyticsPurchases.get(1).getActive());
            Assert.assertEquals(airlyticsPurchases.get(1).getSubscriptionStatus(),
                    PurchaseConsumer.SubscriptionStatus.EXPIRED.getStatus());
            Assert.assertFalse(airlyticsPurchases.get(2).getActive());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void billingIssuePurchaseShoulNotBeActive() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "cancaled_after_purchased.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 2);
            Assert.assertEquals(airlyticsPurchases.get(1).getActive(), false);


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void onPurchaseTheSameProductAfterTrialShouldCreateNewPurchaseRow() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "same_product_trial_after_expiration.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 8);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void upgradedReferenceShouldPointToPurchaseId() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "updated_to_from_reference_bug.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 2);
            Assert.assertEquals(airlyticsPurchases.get(1).getPurchaseIdUpgradedFrom(), airlyticsPurchases.get(0).getId());


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void purchaseAfterTrialShouldHaveTrialStartAndTrialEndDate() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchase_after_trial.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 2);
            Assert.assertNull(airlyticsPurchases.get(1).getTrialEndDate());
            Assert.assertNull(airlyticsPurchases.get(1).getTrialStartDate());
            Assert.assertNotNull(airlyticsPurchases.get(0).getTrialEndDate());
            Assert.assertNotNull(airlyticsPurchases.get(0).getTrialStartDate());


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
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "purchased_after_trial_expired.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 7);
            Assert.assertEquals(false, airlyticsPurchases.get(2).getAutoRenewStatus());
            Assert.assertEquals(false, airlyticsPurchases.get(3).getAutoRenewStatus());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void gracePeriodTestShouldBeFalse() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "grace_period_test.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 1);
            Assert.assertEquals(false, airlyticsPurchases.get(0).getGrace());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void oneYear1PeriodShouldBeTwo() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "twp_period_1.year.1.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 7);


        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void oldMonthlyInteractiveRenewalShouldBeNewPurchaseIfGapIsMoreThanTwoWeeks() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "old_monthly_interactive_renewal.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 2);
            Assert.assertEquals("240000287879924", airlyticsPurchases.get(0).getId());
            Assert.assertEquals("240000287879926", airlyticsPurchases.get(1).getId());

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void oldMonthlyInteractiveRenewalShouldNotBeNewPurchaseIfGapIsLessThanTwoWeeks() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.APP);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "old_monthly_possible_billing_issue.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();
            Assert.assertEquals(airlyticsPurchases.size(), 1);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void startPeriodDateShouldBeStartRenewalDate() {
        PurchaseConsumer.PurchaseMetaData metaData = new PurchaseConsumer.PurchaseMetaData();
        metaData.setType(PurchaseConsumer.PurchaseMetaData.Type.NOTIFICATION);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "start_period_bug.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 3);
            Assert.assertEquals(1608701961000L, airlyticsPurchases.get(2).getPeriodStartDate().longValue(), 0L);

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        try {
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(readTestReceipt(TEST_DATE_FOLDER + File.separator + "start_period_bug2.json"), IOSubscriptionPurchase.class);
            List<AirlyticsPurchase> airlyticsPurchases = new IOSAirlyticsPurchaseState(
                    ioSubscriptionPurchase,
                    metaData,
                    inAppAirlyticsProducts
            ).getUpdatedAirlyticsPurchase();

            Assert.assertEquals(airlyticsPurchases.size(), 2);
            Assert.assertEquals(1613774030000L, airlyticsPurchases.get(1).getPeriodStartDate().longValue(), 0L);

        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    private String readTestReceipt(String path) throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(path)) {
            return IOUtils.toString(Objects.requireNonNull(inputStream), StandardCharsets.UTF_8);
        }
    }
}
