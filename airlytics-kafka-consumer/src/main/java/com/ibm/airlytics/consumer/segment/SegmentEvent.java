package com.ibm.airlytics.consumer.segment;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.consumer.userdb.UserEvent;
import com.segment.analytics.messages.IdentifyMessage;
import com.segment.analytics.messages.MessageBuilder;
import com.segment.analytics.messages.TrackMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SegmentEvent extends UserEvent {
    private static final Logger LOGGER = Logger.getLogger(SegmentEvent.class.getName());

    public SegmentEvent(ConsumerRecord<String, JsonNode> record) {
        super(record);
    }

    private Object attributeToObject(String name, JsonNode node) {
        switch (node.getNodeType()) {
            case BOOLEAN:
                return node.asBoolean();
            case NULL:
                return null;
            case NUMBER:
                if (node.canConvertToLong()) return node.asLong();
                return node.asDouble();
            case STRING:
                return node.asText();
            default:
                LOGGER.error("Segment consumer does not support the node type " + node.getNodeType().name()
                             + " of attribute " + name);
                return null;
        }
    }

    private void addAttributesToMap(Map<String, Object> attributeMap) {
        // Go over all attributes in the event
        Iterator<Map.Entry<String, JsonNode>> attributeIterator = getAttributes().fields();

        while (attributeIterator.hasNext()) {
            Map.Entry<String, JsonNode> attribute = attributeIterator.next();
            attributeMap.put(attribute.getKey(), attributeToObject(attribute.getKey(), attribute.getValue()));
        }
    }

    public MessageBuilder getSegmentMessageBuilder() {
        Map<String, Object> eventProperties = new HashMap<>();
        addAttributesToMap(eventProperties);

        if (getEventName().equals("stream-results") || getEventName().equals("user-attributes")) {
            return IdentifyMessage.builder()
                    .userId(getUserId())
                    .timestamp(new Date(getEventTime().getTime()))
                    .traits(eventProperties);
        }
        else {
            eventProperties.put("eventId", getEventId());
            eventProperties.put("schemaVersion", getSchemaVersion());
            eventProperties.put("productId", getProductId());
            eventProperties.put("platform", getPlatform());

            return TrackMessage.builder(getEventName())
                    .userId(getUserId())
                    .timestamp(new Date(getEventTime().getTime()))
                    .properties(eventProperties);
        }
    }
}
