package com.ibm.airlytics.consumer.mparticle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import com.ibm.airlytics.consumer.mparticle.transformation.MparticleEventTransformer;
import com.ibm.airlytics.consumer.mparticle.transformation.MparticleTransformationConsumerConfig;
import com.mparticle.model.Batch;
import com.mparticle.model.CustomEvent;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MparticleTransformerTest {

    private static final String CONFIG =
            "{" +
                    "\"jsonObjectAttributeAccepted\":false," +
                    "\"jsonArrayAttributeAccepted\":true," +
                    "\"customDimsAcceptedByDefault\":true," +
                    "\"eventMappings\":[{" +
                    "\"airlyticsEvent\":\"subscription-cancelled\"," +
                    "\"thirdPartyEvent\":\"commerce_event\"," +
                    "\"attributes\":{" +
                    "\"userProperties\":{" +
                    "\"*\":\"*\"" +
                    "}" +
                    "}" +
                    "}," +
                    "{" +
                    "\"airlyticsEvent\":\"subscription-purchased\"," +
                    "\"thirdPartyEvent\":\"commerce_event\"," +
                    "\"attributes\":{" +
                    "\"userProperties\":{" +
                    "\"*\":\"*\"" +
                    "}" +
                    "}" +
                    "}]" +
                    "}";

    private static final ObjectMapper mapper = new ObjectMapper();

    private static MparticleEventTransformer mParticleTransformer;

    @BeforeClass
    public static void createConfig() throws Exception {
        MparticleTransformationConsumerConfig config = mapper.readValue(CONFIG, MparticleTransformationConsumerConfig.class);
        mParticleTransformer = new MparticleEventTransformer(config);
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
                    "\"contentDate\": null," +
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
        Optional<Batch> amp = mParticleTransformer.transform(event);
        assertTrue(amp.isPresent());
        Batch batch = amp.get();
        System.out.println(mapper.writeValueAsString(batch));
        assertEquals(1, batch.getEvents().size());
        assertTrue(batch.getEvents().get(0) instanceof CustomEvent);
        assertEquals(13, ((CustomEvent)batch.getEvents().get(0)).getData().getCustomAttributes().size());
    }

    private static final String SOD_USR_ATTRS_EVENT =
            "{" +
                    "   \"customDimensions\": {}," +
                    "   \"platform\": \"ios\"," +
                    "   \"previousValues\": {" +
                    "      \"launchSource\": \"clean\"" +
                    "   }," +
                    "   \"eventTime\": 1636848363430," +
                    "   \"appVersion\": \"12.15\"," +
                    "   \"productId\": \"7900f30b-8f47-4829-95c5-9d4d6e8564ee\"," +
                    "   \"attributes\": {" +
                    "      \"nullDate\": null," +
                    "      \"launchSource\": \"background\"," +
                    "      \"saleOfDataAuthorization\": \"true\"" +
                    "   }," +
                    "   \"sessionId\": \"789EE268-D46E-4B0B-939D-C84DAE423489\"," +
                    "   \"userId\": \"CA384D6E-1FB8-4DCD-930D-C4DF1FF2BFE2\"," +
                    "   \"sessionStartTime\": 1636848363423," +
                    "   \"schemaVersion\": \"18.0\"," +
                    "   \"eventId\": \"20A4273D-98B6-4775-A075-DCE9A7AA5669\"," +
                    "   \"name\": \"user-attributes\"" +
                    "}";

    @Test
    public void testSod() throws Exception {
        AirlyticsEvent event = mapper.readValue(SOD_USR_ATTRS_EVENT, AirlyticsEvent.class);
        Optional<Batch> amp = mParticleTransformer.transform(event);
        assertTrue(amp.isPresent());
        Batch batch = amp.get();
        System.out.println(mapper.writeValueAsString(batch));
        assertEquals(1, batch.getEvents().size());
        assertTrue(batch.getEvents().get(0) instanceof CustomEvent);
        assertEquals(3, batch.getUserAttributes().size());
    }
}
