package com.ibm.airlytics.consumer.braze;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.amplitude.MappingTest;
import com.ibm.airlytics.consumer.braze.dto.BrazeEntity;
import com.ibm.airlytics.consumer.braze.transformation.BrazeEventTransformer;
import com.ibm.airlytics.consumer.braze.transformation.BrazeTransformationConsumerConfig;
import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Optional;

import static org.junit.Assert.*;

public class BrazeTransformerTest {
    
    private static final String CONFIG = 
            "{" +
                    "\"jsonObjectAttributeAccepted\":false," +
                    "\"jsonArrayAttributeAccepted\":true," +
                    "\"includeEventTypes\":[\"user-attributes\"," +
                    "\"user-attribute-detected\"," +
                    "\"subscription-purchased\"," +
                    "\"subscription-renewed\"," +
                    "\"subscription-upgraded\"," +
                    "\"subscription-cancelled\"," +
                    "\"subscription-renewal-status-changed\"," +
                    "\"video-played\"" +
                    "]," +
                    "\"ignoreNegativePurchases\":false," +
                    "\"ignoreTrialPurchases\":false," +
                    "\"purchaseEvents\":[\"subscription-purchased\"," +
                    "\"subscription-renewed\"," +
                    "\"subscription-upgraded\"," +
                    "\"subscription-cancelled\"" +
                    "]," +
                    "\"eventMappings\":[{" +
                    "\"airlyticsEvent\":\"user-attributes\"," +
                    "\"thirdPartyEvent\":\"USER\"," +
                    "\"userProperties\":{" +
                    "\"premiumTrialId\":\"premiumTrialId\"," +
                    "\"premiumTrialStartDate\":\"premiumTrialStartDate\"," +
                    "\"premiumTrialLengthInDays\":\"premiumTrialLengthInDays\"" +
                    "}" +
                    "}," +
                    "{" +
                    "\"airlyticsEvent\":\"user-attribute-detected\"," +
                    "\"thirdPartyEvent\":\"USER\"," +
                    "\"userProperties\":{" +
                    "\"premiumProductId\":\"premiumProductId\"," +
                    "\"premiumStartDate\":\"premiumStartDate\"," +
                    "\"premiumExpirationDate\":\"premiumExpirationDate\"" +
                    "}" +
                    "}," +
                    "{" +
                    "\"airlyticsEvent\":\"subscription-renewal-status-changed\"," +
                    "\"thirdPartyEvent\":\"USER\"," +
                    "\"userProperties\":{" +
                    "\"status\":\"subscriptionRenewalStatus\"," +
                    "\"eventTime\":\"subscriptionRenewalChangeDate\"" +
                    "}" +
                    "}," +
                    "{" +
                    "\"airlyticsEvent\":\"video-played\"," +
                    "\"thirdPartyEvent\":\"video-played\"" +
                    "}]" +
                    "}";

    private static final ObjectMapper mapper = new ObjectMapper();

    private static BrazeEventTransformer transformer;

    @BeforeClass
    public static void createConfig() throws Exception {
        BrazeTransformationConsumerConfig config = mapper.readValue(CONFIG, BrazeTransformationConsumerConfig.class);
        transformer = new BrazeEventTransformer(config);
    }

    @Test
    public void testAll() throws Exception {
        File f = new File("src/test/resources/braze_transformer_test_data/subscription-purchased.json");
        String s = FileUtils.readFileToString(f, "UTF-8");
        AirlyticsEvent airEvent = mapper.readValue(s, AirlyticsEvent.class);
        Optional<BrazeEntity> optBrazeEvent = transformer.transform(airEvent);
        assertTrue(optBrazeEvent.isPresent());
        BrazeEntity brazeEvent = optBrazeEvent.get();
        assertTrue(brazeEvent.getKind() == BrazeEntity.Kind.PURCHASE);
        System.out.println(mapper.writeValueAsString(brazeEvent.toMap()));

        f = new File("src/test/resources/braze_transformer_test_data/subscription-renewed.json");
        s = FileUtils.readFileToString(f, "UTF-8");
        airEvent = mapper.readValue(s, AirlyticsEvent.class);
        optBrazeEvent = transformer.transform(airEvent);
        assertTrue(optBrazeEvent.isPresent());
        brazeEvent = optBrazeEvent.get();
        assertTrue(brazeEvent.getKind() == BrazeEntity.Kind.PURCHASE);
        System.out.println(mapper.writeValueAsString(brazeEvent.toMap()));

        f = new File("src/test/resources/braze_transformer_test_data/subscription-upgraded.json");
        s = FileUtils.readFileToString(f, "UTF-8");
        airEvent = mapper.readValue(s, AirlyticsEvent.class);
        optBrazeEvent = transformer.transform(airEvent);
        assertTrue(optBrazeEvent.isPresent());
        brazeEvent = optBrazeEvent.get();
        assertTrue(brazeEvent.getKind() == BrazeEntity.Kind.PURCHASE);
        System.out.println(mapper.writeValueAsString(brazeEvent.toMap()));

        f = new File("src/test/resources/braze_transformer_test_data/subscription-cancelled.json");
        s = FileUtils.readFileToString(f, "UTF-8");
        airEvent = mapper.readValue(s, AirlyticsEvent.class);
        optBrazeEvent = transformer.transform(airEvent);
        assertTrue(optBrazeEvent.isPresent());
        brazeEvent = optBrazeEvent.get();
        assertTrue(brazeEvent.getKind() == BrazeEntity.Kind.PURCHASE);
        System.out.println(mapper.writeValueAsString(brazeEvent.toMap()));

        airEvent = mapper.readValue(MappingTest.TEST_LOCATIONS_EVENT, AirlyticsEvent.class);
        optBrazeEvent = transformer.transform(airEvent);
        assertFalse(optBrazeEvent.isPresent());

        airEvent.getAttributes().put("premiumTrialId", "test");
        optBrazeEvent = transformer.transform(airEvent);
        assertTrue(optBrazeEvent.isPresent());
        brazeEvent = optBrazeEvent.get();
        assertTrue(brazeEvent.getKind() == BrazeEntity.Kind.USER);
        System.out.println(mapper.writeValueAsString(brazeEvent.toMap()));

        airEvent.setName("video-played");
        airEvent.getAttributes().put("playMethod", "auto");
        optBrazeEvent = transformer.transform(airEvent);
        assertFalse(optBrazeEvent.isPresent());

        airEvent.getAttributes().put("playMethod", "user");
        optBrazeEvent = transformer.transform(airEvent);
        assertTrue(optBrazeEvent.isPresent());
        brazeEvent = optBrazeEvent.get();
        assertTrue(brazeEvent.getKind() == BrazeEntity.Kind.EVENT);
        System.out.println(mapper.writeValueAsString(brazeEvent.toMap()));

        airEvent.setName("subscription-renewal-status-changed");
        airEvent.getAttributes().put("status", Boolean.TRUE);
        optBrazeEvent = transformer.transform(airEvent);
        assertTrue(optBrazeEvent.isPresent());
        brazeEvent = optBrazeEvent.get();
        assertTrue(brazeEvent.getKind() == BrazeEntity.Kind.USER);
        System.out.println(mapper.writeValueAsString(brazeEvent.toMap()));


        airEvent.getAttributes().put("previousStatus", Boolean.TRUE);
        optBrazeEvent = transformer.transform(airEvent);
        assertFalse(optBrazeEvent.isPresent());
    }
}
