package com.ibm.airlytics.consumer.purchase.android;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.consumer.purchase.AirlyticsInAppProduct;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;
import com.ibm.airlytics.consumer.purchase.PurchaseConsumer;
import com.ibm.airlytics.consumer.purchase.events.*;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class AndroidPublishedSubscriptionEventsTest extends AndroidAirlyticsPurchaseStateTest {

    private static final String TEST_DATE_FOLDER = "android_consumer_test_data";
    private static final long maxSubscriptionEventAge = 31536000000L;


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
        metaData.setNotificationType(AndroidPurchaseConsumer.NotificationType.EXPIRED.name());

        try {

            JSONObject subscriptionPurchaseHashRawObject = new JSONObject(readTestPurchase(TEST_DATE_FOLDER + File.separator + "upgraded-subscription.json"));

            AirlyticsPurchase airlyticsPurchase = new AndroidAirlyticsPurchaseState(
                    createMockedSubscriptionPurchase(subscriptionPurchaseHashRawObject),
                    metaData,
                    inAppAirlyticsProducts.get("product.adfree")
            ).getUpdatedPurchase();
            airlyticsPurchase.setProduct("product.adfree");


            Map<String, BaseSubscriptionEvent> duplications = new HashMap<>();


            airlyticsPurchase.setUserId("");
            airlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(airlyticsPurchase.getProduct()).getPriceUSDMicros());

            try {
                List<BaseSubscriptionEvent> subscriptionEvents = purchaseConsumer.getSubscriptionEventsPerPurchase(airlyticsPurchase,
                        null, "", "", inAppAirlyticsProducts,
                        null, "android", maxSubscriptionEventAge);

                subscriptionEvents = purchaseConsumer.removeDuplications(subscriptionEvents, duplications);
                Assert.assertEquals(0, purchaseConsumer.removeDuplications(subscriptionEvents, duplications).size());
                Assert.assertEquals(2, subscriptionEvents.size());
                SubscriptionPurchasedEvent subscriptionPurchasedEvent = (SubscriptionPurchasedEvent) subscriptionEvents.get(0);
                SubscriptionUpgradedEvent subscriptionUpgradedEvent = (SubscriptionUpgradedEvent) subscriptionEvents.get(1);

            } catch (IllegalSubscriptionEventArguments illegalSubscriptionEventArguments) {
                illegalSubscriptionEventArguments.printStackTrace();
            }
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }
}
