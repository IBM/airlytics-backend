package com.ibm.weather.airlytics.amplitude.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Abstract class containing common methods for all event transformers
 */
public class BasicEventTransformer {

    protected static final ObjectMapper mapper = new ObjectMapper();

    @Value("${airlock.airlytics.env:}")
    protected String airlyticsEnvironment;

    @Value("${airlock.airlytics.product:}")
    protected String airlyticsProduct;

    @Value("${airlock.airlytics.deployment:}")
    protected String airlyticsDeployment;

    @Value("classpath:transformerConfig.json")
    private Resource mappingsResource;

    protected GenericEventMappingConfig config;

    public BasicEventTransformer() {
    }

    @PostConstruct
    public void init() throws IOException {
        GenericEventMappingConfig config = readConfig();

        if(!isValid(config)) throw new IllegalArgumentException("Invalid third party event mappings config");

        this.config = config;
    }

    public boolean isValid(GenericEventMappingConfig config) {

        if(config == null) return false;

        if(config.getEventMappings() != null) {
            Optional<EventMappingConfig> invalid =
                    config.getEventMappings()
                            .stream()
                            .filter(c -> StringUtils.isBlank(c.getAirlyticsEvent()))
                            .findAny();

            if (invalid.isPresent()) return false;

            invalid =
                    config.getEventMappings()
                            .stream()
                            .filter(c -> StringUtils.isBlank(c.getThirdPartyEvent()))
                            .findAny();

            return !invalid.isPresent();
        }
        return true;
    }

    protected void applyPropertyFilters(Map<String, Object> properties, List<String> ignored, List<String> included, PropertyMappingConfig mappings) {
        if (ignored != null) {
            ignored.forEach(aName -> {
                boolean isNotMappedInEventProps =
                        mappings == null ||
                                mappings.getEventProperties() == null ||
                                !mappings.getEventProperties().containsKey(aName);
                boolean isNotMappedInUserProps =
                        mappings == null ||
                                mappings.getUserProperties() == null ||
                                !mappings.getUserProperties().containsKey(aName);

                if (isNotMappedInEventProps && isNotMappedInUserProps) {
                    properties.remove(aName);
                }
            });
        }

        if (included != null) {
            List<String> removed =
                    properties.keySet()
                            .stream()
                            .filter(k -> {
                                boolean isIncluded = included.contains(k);

                                if (isIncluded) {
                                    return false;
                                }
                                boolean isNotMappedInEventProps =
                                        mappings == null ||
                                                mappings.getEventProperties() == null ||
                                                !mappings.getEventProperties().containsKey(k);
                                boolean isNotMappedInUserProps =
                                        mappings == null ||
                                                mappings.getUserProperties() == null ||
                                                !mappings.getUserProperties().containsKey(k);

                                return isNotMappedInEventProps && isNotMappedInUserProps;
                            })
                            .collect(Collectors.toList());
            removed.forEach(properties::remove);
        }
    }

    private GenericEventMappingConfig readConfig() throws IOException {
        String json = IOUtils.toString(mappingsResource.getInputStream(), "UTF-8");
        GenericEventMappingConfig config = mapper.readValue(json, GenericEventMappingConfig.class);
        return config;
    }
}
