package com.ibm.airlytics.consumer.amplitude;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.amplitude.dto.AmplitudeEvent;
import com.ibm.airlytics.consumer.amplitude.transformation.AmplitudeEventTransformer;
import com.ibm.airlytics.consumer.amplitude.transformation.AmplitudeTransformationConsumerConfig;
import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.Reader;
import java.nio.file.Files;
import java.util.*;

import static org.junit.Assert.*;

public class AmplitudeTransformerTest {
    
    private static final String CONFIG = 
            "{" +
                    "\"jsonObjectAttributeAccepted\":false," +
                    "\"jsonArrayAttributeAccepted\":true," +
                    "\"ignoreEventTypes\":[\"location-viewed\"," +
                    "\"session-start\"," +
                    "\"session-end\"," +
                    "\"stream-results\"," +
                    "\"in-app-message-displayed\"," +
                    "\"in-app-message-interacted\"," +
                    "\"messaging-campaign-interacted\"," +
                    "\"purchase-renewed\"," +
                    "\"purchase-expired\"," +
                    "\"notification-received\"," +
                    "\"notification-interacted\"," +
                    "\"asset-viewed\"," +
                    "\"ad-impression\"]," +
                    "\"ignoreEventAttributes\":[\"osVersion\"," +
                    "\"deviceModel\"," +
                    "\"deviceLanguage\"," +
                    "\"pushToken\"," +
                    "\"upsId\"," +
                    "\"thirdPartyId\"," +
                    "\"premium\"," +
                    "\"premiumProductId\"," +
                    "\"premiumStartDate\"," +
                    "\"premiumExpirationDate\"," +
                    "\"encodedReceipt\"," +
                    "\"purchases\"," +
                    "\"widgets\"," +
                    "\"previousValues\"]," +
                    "\"customDimsAcceptedByDefault\":true," +
                    "\"eventMappings\":[{" +
                    "\"airlyticsEvent\":\"user-attributes\"," +
                    "\"thirdPartyEvent\":\"$identify\"," +
                    "\"attributes\":{" +
                    "\"userProperties\":{" +
                    "\"*\":\"*\"" +
                    "}" +
                    "}" +
                    "}," +
                    "{" +
                    "\"airlyticsEvent\":\"app-launch\"," +
                    "\"thirdPartyEvent\":\"$identify\"," +
                    "\"attributes\":{" +
                    "\"userProperties\":{" +
                    "\"source\":\"launchSource\"" +
                    "}" +
                    "}" +
                    "}," +
                    "{" +
                    "\"airlyticsEvent\":\"user-attribute-detected\"," +
                    "\"thirdPartyEvent\":\"$identify\"," +
                    "\"attributes\":{" +
                    "\"userProperties\":{" +
                    "\"premium\":\"premium\"," +
                    "\"premiumProductId\":\"premiumProductId\"," +
                    "\"premiumStartDate\":\"premiumStartDate\"," +
                    "\"premiumExpirationDate\":\"premiumExpirationDate\"" +
                    "}" +
                    "}" +
                    "}]" +
                    "}";

    private static final ObjectMapper mapper = new ObjectMapper();

    private static AmplitudeEventTransformer transformer;

    @BeforeClass
    public static void createConfig() throws Exception {
        AmplitudeTransformationConsumerConfig config = mapper.readValue(CONFIG, AmplitudeTransformationConsumerConfig.class);
        transformer = new AmplitudeEventTransformer(config);
    }

    @Test
    public void testOsVersion() throws Exception {
        List<String> ampEvents = new LinkedList<>();
        File f = new File("src/test/resources/amplitude_transformer_test_data/os_version_test.csv");

        CSVParser parser = new CSVParserBuilder()
                .withSeparator(',')
                .build();

        try (
        Reader reader = Files.newBufferedReader(f.toPath());

        CSVReader csvReader = new CSVReaderBuilder(reader)
                .withSkipLines(1)
                .withCSVParser(parser)
                .build();
        ) {
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                AirlyticsEvent event = mapper.readValue(line[1], AirlyticsEvent.class);
                Optional<AmplitudeEvent> amp = transformer.transform(event);

                if(amp.isPresent()) {
                    ampEvents.add(mapper.writeValueAsString(amp.get()));
                }
            }
        }
        System.out.println(ampEvents.toString());
    }

    private static final String CUSTOM_DIMS_EVENT =
            "{" +
                    "\"sessionId\": \"aaa86bb3-4b9a-3cc3-a671-fe2337f93764\"," +
                    "\"schemaVersion\": \"3.0\"," +
                    "\"sessionStartTime\": 1610907145557," +
                    "\"attributes\": {" +
                    "\"cardIndex\": 2," +
                    "\"cardTitle\": null," +
                    "\"cardId\": \"planning-card\"," +
                    "\"timeInView\": 3.0335030555725098" +
                    "}," +
                    "\"customDimensions\": {" +
                    "\"viewedState\": null," +
                    "\"launchSource\": \"widget_forecast_medium\"," +
                    "\"localSevere\": false," +
                    "\"contentMode\": null," +
                    "\"viewedDMA\": null," +
                    "\"viewedCountry\": \"IT\"," +
                    "\"viewedWeatherCondition\": 31" +
                    "}," +
                    "\"productId\": \"7900f30b-8f47-4829-95c5-9d4d6e8564ee\"," +
                    "\"appVersion\": \"12.5\"," +
                    "\"eventTime\": 1610907147556," +
                    "\"name\": \"card-viewed\"," +
                    "\"eventId\": \"998d86ff-225b-42a0-ae17-3f03e8be0d40\"," +
                    "\"userId\": \"aaa494d3-2780-3e2f-87a5-e1d064777707\"," +
                    "\"platform\": \"ios\"" +
                    "}";

    @Test
    public void testCustomDims() throws Exception {
        AirlyticsEvent event = mapper.readValue(CUSTOM_DIMS_EVENT, AirlyticsEvent.class);
        Optional<AmplitudeEvent> amp = transformer.transform(event);
        assertTrue(amp.isPresent());
        AmplitudeEvent aEvent = amp.get();
        System.out.println(mapper.writeValueAsString(aEvent));
        assertEquals(14, aEvent.getEvent_properties().size());
    }

    private static final String PAGE_VIEWED_EVENT = "{" +
            "   \"eventId\": \"1BA90C8F-6FA0-4FF6-8AF1-7C9E90D01526\"," +
            "   \"sessionId\": \"BD008852-48BD-4A27-B4D9-E1FB200DAE91\"," +
            "   \"attributes\": {" +
            "      \"pageId\": \"daily\"," +
            "      \"source\": \"planning-card\"" +
            "   }," +
            "   \"customDimensions\": {" +
            "      \"deviceTimeZone\": -18000," + // -5:00
            "      \"viewedState\": \"TN\"" +
            "   }," +
            "   \"eventTime\": 1629345600000," + // Date and time (GMT): Thursday, August 19, 2021 4:00:00 AM
            "   \"productId\": \"7900f30b-8f47-4829-95c5-9d4d6e8564ee\"," +
            "   \"name\": \"page-viewed\"," +
            "   \"appVersion\": \"12.10\"," +
            "   \"schemaVersion\": \"3.0\"," +
            "   \"platform\": \"ios\"," +
            "   \"userId\": \"CD84DDED-6E89-4A6D-89B2-96CC407EAACC\"," +
            "   \"sessionStartTime\": 1629345600000" +
            "}";

    @Test
    public void testUninstalled() throws Exception {
        AirlyticsEvent event = mapper.readValue(PAGE_VIEWED_EVENT, AirlyticsEvent.class);
        Optional<AmplitudeEvent> amp = transformer.transform(event);
        assertTrue(amp.isPresent());
        AmplitudeEvent aEvent = amp.get();
        System.out.println(mapper.writeValueAsString(aEvent));
        assertNotNull(aEvent.getUser_properties());
        assertTrue(aEvent.getUser_properties().containsKey("isUninstalled"));
        assertEquals(aEvent.getUser_properties().get("isUninstalled"), Boolean.FALSE);
    }

    @Test
    public void testDeviceTime() throws Exception {
        AirlyticsEvent event = mapper.readValue(PAGE_VIEWED_EVENT, AirlyticsEvent.class);
        Optional<AmplitudeEvent> amp = transformer.transform(event);
        assertTrue(amp.isPresent());
        AmplitudeEvent aEvent = amp.get();
        System.out.println(mapper.writeValueAsString(aEvent));
        assertNotNull(aEvent.getEvent_properties());
        assertEquals(23, aEvent.getEvent_properties().get("hourOfDay"));
        assertEquals("Wed", aEvent.getEvent_properties().get("dayOfWeek"));
    }

    private static final String APP_VERSIONS =
            "10.24.0,10.20.0,10.10.0,10.17,10.13.1,10.15.0,10.9.0,10.21.0," +
                    "10.25,10.25.0,10.25.1,1.0,10.13.0,10.12.0,10.9,10.16.0,10.18.0,10.27.0,10.19.0," +
                    "10.32.0,10.14.0,10.30.0,10.11.0,10.33.0,10.34.0,10.31.0,10.17.0,10.26.0,10.22.0,10.29.0,10.23.0,10.28.0";

    @Test
    public void testAppVersions() throws Exception {
        List<String> lst = Arrays.asList(APP_VERSIONS.split(","));
        lst.sort((s1, s2) -> compareVersions(s1, s2));
        lst.forEach(System.out::println);
    }

    /**
     * return res < 0 if s1<s2, 0 if s1===s2, res > 0 if s1>s2
     */
    public static int compareVersions(String s1, String s2) {
        if ((s1 == null || s1.equals("")) && (s2 == null || s2.equals("")))
            return 0;
        if (s1 == null || s1.equals(""))
            return 1;
        if (s2 == null || s2.equals(""))
            return -1;
        String[] s1Array = s1.split("\\.");
        String[] s2Array = s2.split("\\.");
        int numPartsToCompare = Math.min(s1Array.length, s2Array.length);
        for (int i = 0; i < numPartsToCompare; i++) {
            // try to compare numeric
            try {
                int s1Val = Integer.parseInt(s1Array[i]);
                int s2Val = Integer.parseInt(s2Array[i]);
                if (s1Val != s2Val) {
                    return s1Val - s2Val;
                }
            }
            // compare Strings
            catch (NumberFormatException e1) {
                String s1Val = s1Array[i];
                String s2Val = s2Array[i];
                if (s1Val.compareTo(s2Val) != 0) {
                    return s1Val.compareTo(s2Val);
                }
            }
        }
        if (s1Array.length > s2Array.length) {
            // check for the longer array, that all the nodes are negligible (0 or empty string)
            for (int j = numPartsToCompare; j < s1Array.length; j++) {
                if (!s1Array[j].equals("") && !s1Array[j].equals("0")) {
                    return 1;
                }
            }
        } else if (s2Array.length > s1Array.length) {
            // check for the longer array, that all the nodes are negligible (0 or empty string)
            for (int k = numPartsToCompare; k < s2Array.length; k++) {
                if (!s2Array[k].equals("") && !s2Array[k].equals("0")) {
                    return -1;
                }
            }
        }
        return 0;
    }

}
