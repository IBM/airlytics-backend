# Airlytics Service Commons
Reusable components and utilities for Airlytics microservices

## Base Airlock Client

Package **com.ibm.weather.airlytics.common.airlock** contains classes that help implementing loading Airlock feature 
configuration and communicating with Airlock BE API.

**BaseAirlockClient** is an abstract Spring component class implementing obtaining the initial or extended authentication token from
Airlock BE API, and using the token ifurther requests to the API.

**BaseAirlockFeatureConfigManager** is a Spring component class implementing basic functionality for obtaining Airlock 
feature configuration. An Airlytics service may extend this class to add configuration JSON deserialization into 
a service-specific POJO. E.g.
```java
@Component
public class AirCohortsFeatureConfigManager extends BaseAirlockFeatureConfigManager {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public AirCohortsAirlockConfig readFromAirlockFeature() throws IOException {
        String configuration = getAirlockConfiguration();
        return objectMapper.readValue(configuration, AirCohortsAirlockConfig.class);
    }
}
```

## AWS Athena DB Integration

Package **com.ibm.weather.airlytics.common.athena** contains classes that help connecting to AWS Athena DB and running queries.

**AthenaDriver** is a Spring component that implements basic operations in Athena DB: building AWS SDK's Athena client, 
asynchronously submitting a query, waiting for and processing query results.

**AthenaBasicDao** is an abstract class using AthenaDriver and providing a higher level interface for submitting Athena queries 
and processing their results, including some standard queries (e.g. dropping a temp table).

**QueryBuilder** interface with **AbstractBasicQueryBuilder**, **AbstractPartitionQueryBuilder**, and **AbstractPartitionDayQueryBuilder**, 
**PartitionDayConditionQueryBuilder** are intended to be implemented by Airlytics services that run SQL queries in Airlytics 
Atena event tables and rely on the tables' standard structure using user's partion and event day for table partitioning.

## Kafka Integration

Package **com.ibm.weather.airlytics.common.kafka** contains classes that help working with Airlytics Kafka topics.

Package **com.ibm.weather.airlytics.common.kafka.serdes** contains standard serializers and deserializers used by Airlytics services.

**Hashing** is a utility class to produce haches f IDs using standard algorythms, including Murmur2 used to calculate user's shard and partition.

**KafkaProducer** is a utility class used to send events to Airlytics Kafka topics.

## Retryable Rest Template

Package **com.ibm.weather.airlytics.common.rest** contains Spring components that allow Airlytics services for enabeling 
and configuring automatic retries when talking to 3rd party APIs. For that, one should import **RetryableRestTemplateConfiguration**, e.g.
```java
@Configuration
@Import(RetryableRestTemplateConfiguration.class)
public class MyApplicationConfiguration {
    // ...
}
```
When configuration is enabled, one may start using **** for RESTful operations, e.g.

```java
@Component
public class MyServiceClient {
    // ...
    @Autowired
    private RetryableRestTemplate restTemplate;
    // ...
    public Optional<MyServiceResponse> getWhateverFromMyService() {
        ResponseEntity<MyServiceResponse> responseEntity = null;
        // ...
        responseEntity = restTemplate.execute(url, HttpMethod.GET, request, MyServiceResponse.class);
        // ...
        return Optional.ofNullable(responseEntity);
    }
}
```

## Common DTOs

**AirlyticsEvent** is a POJO representation of a standard Airlytics event, with Jackson JSON annotations that ensure proper 
JSON serialization/deserialization using Jackson parser.

**BasicAthenaConfig** is a POJO with a set of essential configurations for connecting to AWS Athena DB. It is used by
**AthenaDriver** (see above).

**AthenaAirlockConfig** extends BasicAthenaConfig to introduce common configurations for the services that process data in Athena.