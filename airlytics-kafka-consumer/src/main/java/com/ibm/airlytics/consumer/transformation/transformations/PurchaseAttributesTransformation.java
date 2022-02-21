package com.ibm.airlytics.consumer.transformation.transformations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.transformation.TransformedUserEvent;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

public class PurchaseAttributesTransformation extends Transformation {

    private final static String TRANSFORMATION_ID = "PURCHASE_ATTRIBUTES_TRANSFORMATION";
    private final static String ANDROID_PURCHASE_USER_ATTRIBUTE = "purchases";
    private final static String IOS_PURCHASE_USER_ATTRIBUTE = "encodedReceipt";

    public static String transformationId(){
        return TRANSFORMATION_ID;
    }

    @Override
    public Optional<JsonNode> transform(TransformedUserEvent originalEvent, ObjectMapper mapper) {

        boolean found = false;
        Iterator<Map.Entry<String, JsonNode>> attributeIterator = originalEvent.getAttributes().fields();

        while (attributeIterator.hasNext()) {
            Map.Entry<String, JsonNode> attribute = attributeIterator.next();

            if (attribute.getKey().equalsIgnoreCase(IOS_PURCHASE_USER_ATTRIBUTE) ||
                    attribute.getKey().equalsIgnoreCase(ANDROID_PURCHASE_USER_ATTRIBUTE)){
                found = true;
            } else {
                // TODO: remove unnecessary attribute
            }
        }

        if (found){
            return Optional.of(mapper.valueToTree(originalEvent));
        }
        return Optional.empty();
    }
}
