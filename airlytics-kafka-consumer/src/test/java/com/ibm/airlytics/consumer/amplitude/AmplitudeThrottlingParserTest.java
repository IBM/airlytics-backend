package com.ibm.airlytics.consumer.amplitude;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class AmplitudeThrottlingParserTest {

    private static final String AMP_RESP =
            "{\"code\":429," +
                    "\"exceeded_daily_quota_users\":{}," +
                    "\"throttled_events\":[0,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49]," +
                    "\"throttled_users\":{\"E236FE8E-1A9F-4BCB-ABFB-331B1DEA0821\":30}," +
                    "\"throttled_devices\":{\"E236FE8E-1A9F-4BCB-ABFB-331B1DEA0821\":30}," +
                    "\"error\":\"Too many requests for some devices and users\",\"eps_threshold\":30,\"exceeded_daily_quota_devices\":{}}";
    final private ObjectMapper mapper = new ObjectMapper();
    final private List<Integer> expected =
            Arrays.asList(0,1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49);

    @Test
    public void test() throws Exception {
        JsonNode resp = mapper.readTree(AMP_RESP);
        List<Integer> parsed = new LinkedList<>();

        if(resp.has("throttled_events")) {
            ArrayNode lst = (ArrayNode)resp.get("throttled_events");
            lst.elements().forEachRemaining(idx -> parsed.add(idx.asInt()));
        }
        assertEquals(expected.size(), parsed.size());
        assertTrue(expected.containsAll(parsed));
    }

}
