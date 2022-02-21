# Airlytics Kafka Consumer
The project is accumulative implementation of all Kafka consumer types

## Components Overview

### Amplitude Event Forwarding
Base Package: com.ibm.airlytics.consumer.amplitude

This package contains code for Airlytics event forwarding to Amplitude.

#### AmplitudeTransformationConsumer

Base Package: com.ibm.airlytics.consumer.amplitude.transformation

AmplitudeTransformationConsumer is a Kafka consumer that listens to a raw Airlytics events topic for a certain product, 
transforms these events into Amplitude event format, and sends to the product's Amplitude events Kafka topic. The latter 
destination topic is the source for AmplitudeForwardingConsumer (see below).

The transformation rules are highly configurable allowing for filtering out certain events, and event attributes, as well
as changing event and attribute names in the produced Amplitude event. 

See com.ibm.airlytics.consumer.amplitude.transformation.AmplitudeTransformationConsumerConfig and its base classes for 
possible configurations (such as transformation rules, event white- and black-lists, etc.).

See com.ibm.airlytics.consumer.amplitude.transformation.AmplitudeEventTransformer and its base classes for the implemetation 
of the configured transformation logic.

Environment variables configuration example:
```yaml
      containers:
        - name: amplitude-transform-consumer
          image: myimagestore.com/airlytics/consumer:latest
          args: ["amplitude_transform"]
          ports:
            - containerPort: 8084
              name: metrics
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'Hello World App'
            - name: METRICS_PORT
              value:  '8084'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
```

Airlock configuration example:
```json
{
  "producerCompressionType": "snappy",
  "lingerMs": 100,
  "maxPollIntervalMs": 300000,
  "securityProtocol": "SSL",
  "bootstrapServers": "kafka1.mykafka.com:9094,kafka2.mykafka.com:9094",
  "maxPollRecords": 20000,
  "topic": "HelloWorld",
  "consumerGroupId": "AMPLITUDE_TRANSFORM_HELLOWORLD_PROD",
  "jsonArrayAttributeAccepted": true,
  "jsonObjectAttributeAccepted": false,
  "ignoreEventTypes": [
    "session-start",
    "session-end"
  ],
  "ignoreEventAttributes": [
    "osVersion",
    "carrier",
    "deviceModel",
    "deviceLanguage",
    "deviceManufacturer"
  ],
  "customDimsAcceptedByDefault": true,
  "amplitudeEventsTopic": "AmplitudeHelloWorld",
  "eventMappings": [
    {
      "airlyticsEvent": "user-attributes",
      "thirdPartyEvent": "$identify",
      "attributes": {
        "userProperties": {
          "*": "*"
        }
      }
    },
    {
      "airlyticsEvent": "app-launch",
      "thirdPartyEvent": "$identify",
      "attributes": {
        "userProperties": {
          "source": "launchSource"
        }
      }
    }
  ]
}
```

#### AmplitudeForwardingConsumer

Base Package: com.ibm.airlytics.consumer.amplitude.forwarding

AmplitudeForwardingConsumer is a Kafka consumer that listens to Amplitude events Kafka topic for a certain product
(populated by AmplitudeTransformationConsumer, see above). It batches the received events and forward to Amplitude.

See com.ibm.airlytics.consumer.amplitude.forwarding.AmplitudeForwardingConsumerConfig and its base classes for
possible configurations (such as Amplitude end-point, batch size, etc).

See com.ibm.airlytics.consumer.amplitude.forwarding.AmplitudeApiClient for the implemetation of Amplitude API client.

Environment variables configuration example:
```yaml
      containers:
        - name: amplitude-forward-consumer
          image: myimagestore.com/airlytics/consumer:latest
          args: ["amplitude_forward"]
          ports:
            - containerPort: 8084
              name: metrics
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'Hello World App'
            - name: METRICS_PORT
              value:  '8084'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
            - name: AMPLITUDE_KEY
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: amplitude-helloworld-key
```

Airlock configuration example:
```json
{
  "maxPollIntervalMs": 300000,
  "securityProtocol": "SSL",
  "bootstrapServers": "kafka1.mykafka.com:9094,kafka2.mykafka.com:9094",
  "maxPollRecords": 600,
  "topic": "AmplitudeHelloWorld",
  "consumerGroupId": "AMPLITUDE_FORWARD_HELLOWORLD_PROD",
  "amplitudeEventApiPath": "/2/httpapi",
  "percentageUsersForwarded": 10,
  "useEventApi": true,
  "amplitudeBatchApiBatchSize": 1000,
  "amplitudeBatchApiPath": "/batch",
  "amplitudeApiBaseUrl": "https://api.amplitude.com",
  "amplitudeEventApiBatchSize": 100,
  "amplitudeIntegrationEnabled": true
}
```

#### AmplitudeCohortsConsumer

Base package: com.ibm.airlytics.consumer.cohorts.amplitude

AmplitudeCohortsConsumer is a Kafka consumer that listens to Cohorts export Kafka topic (populated by AirCohorts service).
It batches the received user cohort values, transforms into Amplitude "$identify" events, batches them, and forwards to Amplitude.

See com.ibm.airlytics.consumer.amplitude.forwarding.AmplitudeApiClient for the implemetation of Amplitude API client.

Environment variables configuration example:
```yaml
      containers:
        - name: amplitude-cohorts-consumer
          image: myimagestore.com/airlytics/consumer:latest
          args: ["cohorts_amplitude"]
          ports:
            - containerPort: 8084
              name: metrics
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'AirCohorts'
            - name: METRICS_PORT
              value:  '8084'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
            - name: AIRLOCK_KEY
              valueFrom:
                secretKeyRef:
                  name: airlock-api-secret
                  key: airlock-key
            - name: AIRLOCK_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: airlock-api-secret
                  key: airlock-password
            - name: AMPLITUDE_HELLOWORLD_KEY
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: amplitude-helloworld-key
            - name: AMPLITUDE_BYEBYEWORLD_KEY
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: amplitude-goodbyeworld-key
```


Airlock configuration example:
```json
{
  "maxPollIntervalMs": 900000,
  "securityProtocol": "SSL",
  "bootstrapServers": "kafka1.mykafka.com:9094,kafka2.mykafka.com:9094",
  "maxPollRecords": 1000,
  "topic": "AirCohorts",
  "apiBaseUrl": "https://api.amplitude.com",
  "consumerGroupId": "COHORTS_AMPLITUDE",
  "amplitudeApiKeys": {
    "12345": "AMPLITUDE_HELLOWORLD_KEY",
    "54321": "AMPLITUDE_BYEBYEWORLD_KEY"
  },
  "amplitudeApiEnabled": true,
  "airlockApiEnabled": true,
  "apiBatchSize": 1000,
  "apiPath": "/batch",
  "airlockApiBaseUrl": "https://airlock.myserver.com/airlock/api",
  "maxWaitForBatchMs": 2000
}
```

### Braze Event Forwarding
Base Package: com.ibm.airlytics.consumer.braze

This package contains code for Airlytics event forwarding to Braze.

#### BrazeTransformationConsumer

Base Package: com.ibm.airlytics.consumer.braze.transformation

BrazeTransformationConsumer is a Kafka consumer that listens to a raw Airlytics events topic for a certain product,
transforms these events into Braze event format, and sends to the product's Braze events Kafka topic. The latter
destination topic is the source for BrazeForwardingConsumer (see below).

The transformation rules are highly configurable allowing for filtering out certain events, and event attributes, as well
as changing event and attribute names in the produced Braze event.

See com.ibm.airlytics.consumer.braze.transformation.BrazeTransformationConsumerConfig and its base classes for
possible configurations (such as transformation rules, event white- and black-lists, etc.).

See com.ibm.airlytics.consumer.braze.transformation.BrazeEventTransformer and its base classes for the implemetation
of the configured transformation logic.

Environment variables configuration example:
```yaml
      containers:
        - name: braze-transform-consumer
          image: myimagestore.com/airlytics/consumer:latest
          args: ["braze_transform"]
          ports:
            - containerPort: 8084
              name: metrics
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'Android Product'
            - name: METRICS_PORT
              value:  '8084'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
```


Airlock configuration example:
```json
{
  "maxPollIntervalMs": 300000,
  "securityProtocol": "SSL",
  "bootstrapServers": "kafka1.mykafka.com:9094,kafka2.mykafka.com:9094",
  "maxPollRecords": 600,
  "topic": "HelloWorld",
  "consumerGroupId": "BRAZE_TRANSFORM_HELLOWORLD_PROD",
  "jsonArrayAttributeAccepted": true,
  "maxPollIntervalMs": 1200000,
  "producerCompressionType": "snappy",
  "jsonObjectAttributeAccepted": false,
  "ignoreNegativePurchases": false,
  "includeEventTypes": [
    "user-attributes",
    "user-attribute-detected",
    "subscription-purchased",
    "subscription-renewed",
    "subscription-upgraded",
    "subscription-cancelled",
    "subscription-renewal-status-changed",
    "video-played"
  ],
  "eventMappings": [
    {
      "airlyticsEvent": "user-attributes",
      "thirdPartyEvent": "USER",
      "attributes": {
        "userProperties": {
          "meteredTrialStartDate": "meteredTrialStartDate",
          "meteredTrialLengthInDays": "meteredTrialLengthInDays",
          "premiumTrialStartDate": "meteredTrialStartDate",
          "premiumTrialLengthInDays": "meteredTrialLengthInDays",
          "meteredTrialId": "meteredTrialId",
          "premiumTrialId": "meteredTrialId"
        }
      }
    },
    {
      "airlyticsEvent": "user-attribute-detected",
      "thirdPartyEvent": "USER",
      "attributes": {
        "userProperties": {
          "premiumStartDate": "premiumStartDate",
          "premium": "premium",
          "premiumTrial": "premiumTrial",
          "premiumExpirationDate": "premiumExpirationDate",
          "premiumProductId": "premiumProductId"
        }
      }
    },
    {
      "airlyticsEvent": "subscription-renewal-status-changed",
      "thirdPartyEvent": "USER",
      "attributes": {
        "userProperties": {
          "eventTime": "premiumAutoRenewChangeDate",
          "status": "premiumAutoRenewStatus"
        }
      }
    },
    {
      "airlyticsEvent": "video-played",
      "thirdPartyEvent": "video-played"
    }
  ],
  "lingerMs": 100,
  "maxPollRecords": 10000,
  "destinationTopic": "BrazeHelloWorld",
  "ignoreTrialPurchases": false,
  "purchaseEvents": [
    "subscription-purchased",
    "subscription-renewed",
    "subscription-upgraded",
    "subscription-cancelled"
  ]
}
```

#### BrazeForwardingConsumer

Base Package: com.ibm.airlytics.consumer.braze.forwarding

BrazeForwardingConsumer is a Kafka consumer that listens to Braze events Kafka topic for a certain product
(populated by BrazeTransformationConsumer, see above). It batches the received events and forward to Braze.

See com.ibm.airlytics.consumer.braze.forwarding.BrazeForwardingConsumerConfig and its base classes for
possible configurations (such as Braze end-point, batch size, etc).

See com.ibm.airlytics.consumer.braze.forwarding.BrazeApiClient for the implemetation of Braze API client.

Environment variables configuration example:
```yaml
      containers:
        - name: braze-forward-consumer
          image: myimagestore.com/airlytics/consumer:latest
          args: ["braze_forward"]
          ports:
            - containerPort: 8084
              name: metrics
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'Hello World App'
            - name: METRICS_PORT
              value:  '8084'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
            - name: BRAZE_KEY
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: braze-helloworld-key
```

Airlock configuration example:
```json
{
  "maxPollIntervalMs": 300000,
  "securityProtocol": "SSL",
  "bootstrapServers": "kafka1.mykafka.com:9094,kafka2.mykafka.com:9094",
  "maxPollRecords": 600,  
  "topic": "BrazeIOSProduct",
  "consumerGroupId": "BRAZE_FORWARD_IOSPRODUCT_PROD",
  "brazeApiBaseUrl": "https://rest.iad-06.braze.com",
  "brazeApiPath": "/users/track",
  "brazeAppId": "1234",
  "percentageUsersForwarded": 10,
  "apiParallelThreads": 250,
  "brazeIntegrationEnabled": true
}
```

#### BrazeCohortsConsumer

Base package: com.ibm.airlytics.consumer.cohorts.braze

BrazeCohortsConsumer is a Kafka consumer that listens to Cohorts export Kafka topic (populated by AirCohorts service).
It batches the received user cohort values, transforms into Braze user attribute events, batches them, and forwards to Braze.

See com.ibm.airlytics.consumer.braze.forwarding.BrazeApiClient for the implemetation of Braze API client.

Environment variables configuration example:
```yaml
      containers:
        - name: braze-cohorts-consumer
          image: myimagestore.com/airlytics/consumer:latest
          args: ["cohorts_braze"]
          ports:
            - containerPort: 8084
              name: metrics
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'AirCohorts'
            - name: METRICS_PORT
              value:  '8084'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
            - name: AIRLOCK_KEY
              valueFrom:
                secretKeyRef:
                  name: airlock-api-secret
                  key: airlock-key
            - name: AIRLOCK_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: airlock-api-secret
                  key: airlock-password
            - name: BRAZE_HELLOWORLD_KEY
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: braze-helloworld-key
            - name: BRAZE_BYEBYEWORLD_KEY
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: braze-goodbyeworld-key
```

Airlock configuration example:
```json
{
  "maxPollIntervalMs": 900000,
  "securityProtocol": "SSL",
  "bootstrapServers": "kafka1.mykafka.com:9094,kafka2.mykafka.com:9094",
  "maxPollRecords": 1000,
  "topic": "AirCohorts",
  "apiBaseUrl": "https://rest.iad-06.braze.com",
  "consumerGroupId": "COHORTS_BRAZE",
  "airlockApiEnabled": true,
  "brazeApiEnabled": true,
  "apiPath": "/users/track",
  "brazeApiKeys": {
    "12345": "BRAZE_HELLOWORLD_KEY",
    "54321": "BRAZE_BYEBYEWORLD_KEY"
  },
  "airlockApiBaseUrl": "https://airlock.myserver.com/airlock/api",
  "maxWaitForBatchMs": 2000
}
```

#### BrazeCurrentsConsumer

Base Package: com.ibm.airlytics.consumer.braze.currents

BrazeCurrentsConsumer is a Kafka consumer that listens to Braze Currents topic populated by a LTV Input consumer with 
messages on new files in Braze Currents S3 bucket. It parses the files (stored by Braze in Avro format), and transforms 
user engagement events into Airlytics events that are then forwarded to the Airlytics Proxy. This consumer uses a local 
disk to store its state that can be restored in case of JVM restart in order to avoid re-sending duplicates.

See com.ibm.airlytics.consumer.braze.currents.BrazeCurrentsConsumerConfig and its base classes for
possible configurations (such as Airlytics Proxy end-point, batch size, etc).

Environment variables configuration example:
```yaml
        - name: braze-currents-consumer
          image: myimagestore.com/airlytics/consumer:latest
          args: ["braze_currents"]
          volumeMounts:
            - mountPath: "/usr/src/app/data"
              name: persistence-pv-storage
          ports:
            - containerPort: 8084
              name: metrics
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'Android Product'
            - name: METRICS_PORT
              value:  '8084'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
```

Airlock configuration example:
```json
{
  "securityProtocol": "SSL",
  "bootstrapServers": "kafka1.mykafka.com:9094,kafka2.mykafka.com:9094",
  "maxPollIntervalMs": 900000,
  "maxPollRecords": 100,
  "topic": "BrazeCurrents",
  "consumerGroupId": "BRAZE_CURRENTS_CONSUMER",
  "eventProxyIntegrationConfig": {
    "eventApiBatchSize": 500,
    "eventApiEnabled": true,
    "obfuscation": {
      "eventRules": [
        {
          "guidHash": [
            "userId",
            "attributes.brazeUserId",
            "attributes.sendId"
          ],
          "guidRandom": [
            "eventId"
          ],
          "event": "*"
        }
      ],
      "hashPrefix": "aaa"
    }
  },
  "eventApiClientConfig": {
    "eventApiBaseUrl": "https://airlytics.myserver.com",
    "eventApiPath": "/eventproxy/track"
  },
  "includeEventTypes": [
    "dataexport.S3.inner.users.messages.inappmessage.Impression"
  ],
  "percentageUsersSent": 10,
  "progressFolder": "/usr/src/app/data/airlytics-datalake-internal-prod/braze-currents-progress",
  "eventMappings": [
    {
      "airlyticsEvent": "braze-in-app-message-impression",
      "thirdPartyEvent": "dataexport.S3.inner.users.messages.inappmessage.Impression",
      "attributes": {
        "eventProperties": {
          "device_id": "deviceId",
          "canvas_step_name": "canvasStepName",
          "timezone": "timezone",
          "message_variation_id": "messageVariationId",
          "dispatch_id": "dispatchId",
          "canvas_step_id": "canvasStepId",
          "canvas_name": "canvasName",
          "ad_tracking_enabled": "adTrackingEnabled",
          "card_id": "cardId",
          "campaign_name": "campaignName",
          "canvas_variation_name": "canvasVariationName",
          "send_id": "sendId",
          "user_id": "brazeUserId",
          "canvas_variation_id": "canvasVariationId",
          "canvas_id": "canvasId",
          "campaign_id": "campaignId"
        }
      }
    }
  ],
  "products": [
    {
      "schemaVersion": "1.0",
      "eventApiKey": "k1",
      "devEventApiKey": "dk1",
      "id": "123",
      "conditions": {
        "app_id": [
          "234"
        ]
      },
      "platform": "ios"
    }
  ]
}
```

### mParticle Event Forwarding
Base Package: com.ibm.airlytics.consumer.mparticle

This package contains code for Airlytics event forwarding to Amplitude.

#### MparticleTransformationConsumer

Base Package: com.ibm.airlytics.consumer.mparticle.transformation

MparticleTransformationConsumer is a Kafka consumer that listens to a raw Airlytics events topic for a certain product,
transforms these events into mParticle event format, and sends to the product's mParticle events Kafka topic. The latter
destination topic is the source for MparticleForwardingConsumer (see below).

The transformation rules are highly configurable allowing for filtering out certain events, and event attributes, as well
as changing event and attribute names in the produced mParticle event.

See com.ibm.airlytics.consumer.mparticle.transformation.MparticleTransformationConsumerConfig and its base classes for
possible configurations (such as transformation rules, event white- and black-lists, etc.).

See com.ibm.airlytics.consumer.mparticle.transformation.MparticleEventTransformer and its base classes for the implemetation
of the configured transformation logic.

Environment variables configuration example:
```yaml
      containers:
        - name: mparticle-transform-consumer
          image: myimagestore.com/airlytics/consumer:latest
          args: ["mparticle_transform"]
          ports:
            - containerPort: 8084
              name: metrics
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'Hello World App'
            - name: METRICS_PORT
              value:  '8084'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
```

Airlock configuration example:
```json
{
  "maxPollIntervalMs": 300000,
  "securityProtocol": "SSL",
  "bootstrapServers": "kafka1.mykafka.com:9094,kafka2.mykafka.com:9094",
  "maxPollRecords": 600,
  "topic": "HelloWorld",
  "consumerGroupId": "AMPLITUDE_TRANSFORM_HELLOWORLD_PROD",
  "jsonArrayAttributeAccepted": true,
  "jsonObjectAttributeAccepted": false,
  "customDimsAcceptedByDefault": true,
  "amplitudeEventsTopic": "MparticleHelloWorld"
}
```

#### MparticleForwardingConsumer

Base Package: com.ibm.airlytics.consumer.mparticle.forwarding

MparticleForwardingConsumer is a Kafka consumer that listens to mParticle events Kafka topic for a certain product
(populated by MparticleTransformationConsumer, see above). It batches the received events and forward to mParticle 
using its Java SDK.

See com.ibm.airlytics.consumer.amplitude.forwarding.MparticleForwardingConsumerConfig and its base classes for
possible configurations (such as mParticle end-point).

Environment variables configuration example:
```yaml
      containers:
        - name: mparticle-forward-consumer
          image: myimagestore.com/airlytics/consumer:latest
          args: ["mparticle_forward"]
          ports:
            - containerPort: 8084
              name: metrics
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'Hello World App'
            - name: METRICS_PORT
              value:  '8084'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
            - name: MPARTICLE_KEY
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: mparticle-helloworld-key
            - name: MPARTICLE_SECRET
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: mparticle-helloworld-secret
```

Airlock configuration example:
```json
{
  "maxPollIntervalMs": 300000,
  "securityProtocol": "SSL",
  "bootstrapServers": "kafka1.mykafka.com:9094,kafka2.mykafka.com:9094",
  "maxPollRecords": 600,
  "topic": "MparticleHelloWorld",
  "consumerGroupId": "MPARTICLE_FORWARD_HELLOWORLD_PROD",
  "percentageUsersForwarded": 100,
  "mparticleApiBaseUrl": "https://s2s.us2.mparticle.com/v2/",
  "apiParallelThreads": 10,
  "mparticleIntegrationEnabled": true
}
```

#### MparticleCohortsConsumer

Base package: com.ibm.airlytics.consumer.cohorts.mparticle

MparticleCohortsConsumer is a Kafka consumer that listens to Cohorts export Kafka topic (populated by AirCohorts service).
It batches the received user cohort values, transforms into mParticle user attribute update events, batches them, and 
forwards to mParticle using its Java SDK.

Environment variables configuration example:
```yaml
      containers:
        - name: mparticle-cohorts-consumer
          image: myimagestore.com/airlytics/consumer:latest
          args: ["cohorts_mparticle"]
          ports:
            - containerPort: 8084
              name: metrics
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'AirCohorts'
            - name: METRICS_PORT
              value:  '8084'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
            - name: AIRLOCK_KEY
              valueFrom:
                secretKeyRef:
                  name: airlock-api-secret
                  key: airlock-key
            - name: AIRLOCK_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: airlock-api-secret
                  key: airlock-password
            - name: MPARTICLE_HELLOWORLD_KEY
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: mparticle-helloworld-key
            - name: MPARTICLE_HELLOWORLD_SECRET
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: mparticle-helloworld-secret
            - name: MPARTICLE_BYEBYEWORLD_KEY
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: mparticle-goodbyeworld-key
            - name: MPARTICLE_BYEBYEWORLD_SECRET
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: mparticle-goodbyeworld-secret
```

Airlock configuration example:
```json
{
  "maxPollIntervalMs": 300000,
  "securityProtocol": "SSL",
  "bootstrapServers": "kafka1.mykafka.com:9094,kafka2.mykafka.com:9094",
  "maxPollRecords": 600,
  "topic": "AirCohorts",
  "consumerGroupId": "COHORTS_MPARTICLE",
  "mparticleApiBaseUrl": "https://s2s.us2.mparticle.com/v2/",
  "airlockApiEnabled": true,
  "apiParallelThreads": 10,
  "apiRateLimit": 5,
  "mparticleApiKeys": {
    "12345": "MPARTICLE_HELLOWWORLD",
    "54321": "MPARTICLE_BYEBYEWORLD"
  },
  "mparticleIntegrationEnabled": true,
  "airlockApiBaseUrl": "https://airlock.myserver.com/airlock/api",
  "maxWaitForBatchMs": 2000
}
```

### Localytics Forwarding

#### LocalyticsCohortsConsumer

Base package: com.ibm.airlytics.consumer.cohorts.localytics

LocalyticsCohortsConsumer is a Kafka consumer that listens to Cohorts export Kafka topic (populated by AirCohorts service).
It batches the received user cohort values, batches them, transforms into Localytics user profile update CSV, and forwards 
to Localytics.

See com.ibm.airlytics.consumer.cohorts.localytics.LocalyticsClient for the implemetation of Localytics API client.

Environment variables configuration example:
```yaml
      containers:
        - name: localytics-cohorts-consumer
          image: myimagestore.com/airlytics/consumer:latest
          args: ["cohorts_localytics"]
          ports:
            - containerPort: 8084
              name: metrics
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'AirCohorts'
            - name: METRICS_PORT
              value:  '8084'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
            - name: AIRLOCK_KEY
              valueFrom:
                secretKeyRef:
                  name: airlock-api-secret
                  key: airlock-key
            - name: AIRLOCK_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: airlock-api-secret
                  key: airlock-password
            - name: LOCALYTICS_KEY
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: localytics-key
            - name: LOCALYTICS_SECRET
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: localytics-secret
```

### Event Cloning

#### CloningConsumer

Base package: com.ibm.airlytics.consumer.cloning

CloningConsumer is a Kafka consumer that listens to a raw Airlytics events topic for a certain product, and forwards 
these events in small batches to a pre-configured endpoint (typically an Airlytics Proxy of a different environment). 
It is used to clone External Production events into an Internal Test environment. For privacy reasons, one may enable 
and configure event obfuscation. 

See com.ibm.airlytics.consumer.cloning.config.CloningConsumerConfig and its base classes for details on obfuscation configuration, 
as well as other configuration parameters, such as percentage of events forwarded, event black- or white-listing, etc.

Environment variables configuration example:
```yaml
      containers:
        - name: cloning-consumer
          image: myimagestore.com/airlytics/consumer:latest
          args: ["cloning"]
          ports:
            - containerPort: 8084
              name: metrics
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'Hello World App'
            - name: METRICS_PORT
              value:  '8084'
            - name: AIRLYTICS_RAWDATA_SOURCE
              value:  'SUCCESS'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
```

Airlock configuration example:
```json
{
  "maxPollIntervalMs": 300000,
  "securityProtocol": "SSL",
  "bootstrapServers": "kafka1.mykafka.com:9094,kafka2.mykafka.com:9094",
  "maxPollRecords": 600,
  "topic": "HelloWorld",
  "consumerGroupId": "CLONING_HELLOWORLD_PROD",
  "eventApiKey": "MyKey",
  "eventApiRetries": 2,
  "eventApiBaseUrl": "https://airlytics-test.myserver.com",
  "eventApiBatchSize": 50,
  "eventApiRateLimit": 6000,
  "percentageUsersCloned": 10,
  "eventApiParallelThreads": 300,
  "obfuscation": {
    "resendObfuscated": false,
    "eventRules": [
      {
        "guidHash": [
          "userId",
          "sessionId"
        ],
        "guidRandom": [
          "eventId"
        ],
        "event": "*"
      },
      {
        "guidHash": [
          "attributes.pushToken",
          "attributes.upsId"
        ],
        "event": "user-attributes"
      }
    ],
    "hashPrefix": "bbb"
  },
  "eventApiPath": "/eventproxy/track"
}
```

#### KafkaCloningConsumer

Base package: com.ibm.airlytics.consumer.kafkacloning

KafkaCloningConsumer is a Kafka consumer that listens to a raw Airlytics events topic for a certain product, and forwards
these events to another Kafka topic (and possibly another Kafka). For privacy reasons, one may enable
and configure event obfuscation.

See com.ibm.airlytics.consumer.kafkacloning.KafkaCloningConsumerConfig and its base classes for details on obfuscation configuration,
as well as other configuration parameters, such as percentage of events forwarded, event black- or white-listing, etc.


### Purchases

#### IOSPurchaseConsumer

Base package: com.ibm.airlytics.consumer.purchase
The base class com.ibm.airlytics.consumer.purchase.PurchaseConsumer

The consumer responsibility to pull mobile subscription Airlytics events that are injected into  
Kafka appropriate topics.
The mobile subscription events arrive from two sources:
   1. Mobile app (events directly are sent from app as a part of user attribute elements)
   2. Realtime subscription notification from Cloud Pub/Sub topic
   
Both event types arrive to  the Airlytics platform through Airlytics Event Proxy.    
The proxy performs the initial validation and submit them to the iOS or Android topics 
Based on the event types and the source an event arrives from,Event Proxy determines what Kafka topic the event
will be submitted.


com.ibm.airlytics.consumer.purchase.IOSPurchaseConsumer is a Kafka consumer that listens 
to two Kafka events topics [IOS<Product>Purchase, IOS<Product>Notification] 
Each iOS event contains the full subscription receipt history a user did so far.

The main IOSPurchaseConsumer purpose is to recognise the notification type, parse and insert in
purchases related DB table, later this data will be queried by Analytics tools for build various cohorts.

See com.ibm.airlytics.consumer.purchase.PurchaseConsumerConfig the Purchase consumer configuration base classes.
The configuration holds common Consumer parameters plus extra set specific for the Purchase consumer.

Environment variables configuration example:
```yaml
      containers:
        - name: purchase-consumer
          image: myimagestore.com/airlytics/consumer:latest
          args: ["ios_purchase"]
          ports:
            - containerPort: 8084
              name: metrics
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'iOS Product'
            - name: METRICS_PORT
              value:  '8084'
            - name: AIRLYTICS_CONSUMER_NAME
              value:  'ios purchase'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
            - name: USERDB_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: userdb-secret
                      key: airlytics-all-rw

```

Airlock configuration example:
```json
{
    "maxPollIntervalMs": 300000,
    "securityProtocol": "SSL",
    "bootstrapServers": "kafka1.mykafka.com:9094,kafka2.mykafka.com:9094",
    "maxPollRecords": 600, 
    "consumerGroupId":"PURCHASE_CONSUMER_IOSPRODUCT",
    "notificationsTopic":"IOSProductNotifications",
    "eventProxyApiKey":"<eventProxyApiKey>",
    "eventProxyDevApiKey":"<eventProxyDevApiKey>",
    "sqlStatesContinuationPrefixes":[],
    "airlockProductId":"<airlockProductId>",
    "errorsTopic":"IOSProductPurchaseConsumerErrors",
    "topics":["IOSProductPurchases",
    "IOSProductNotifications",
    ],
    "updatePurchaseRenewalStatusInterval":86400,
    "updatePurchaseRenewalStatusInitDelay":3600,
    "updatePurchaseRenewalStatusShortInterval":3600,
    "fullPurchaseUpdateInterval":2592000,
    "fullPurchaseUpdateInitDelay":2592000,
    "purchasesEventsTable":"ios_[product].purchase_events",
    "purchasesEventsDevTable":"ios_[product]_dev.purchase_events",
    "purchasesTable":"ios_[product].purchases",
    "purchasesUsersTable":"ios_[product].purchases_users",
    "purchasesDevTable":"ios_[product]_dev.purchases",
    "purchasesUsersDevTable":"ios_[product]_dev.purchases_users",
    "fixingBuggyRows":true
}
```



#### AndroidPurchaseConsumer


Base package: com.ibm.airlytics.consumer.purchase
The base class com.ibm.airlytics.consumer.purchase.PurchaseConsumer

The consumer responsibility to pull mobile subscription Airlytics events that are injected into  
Kafka appropriate topics.
The mobile subscription events arrive from two sources:
   1. Mobile app (events directly are sent from app as a part of user attribute elements)
   2. Realtime subscription notification from Cloud Pub/Sub topic
   
Both event types arrive to  the Airlytics platform through Airlytics Event Proxy.    
The proxy performs the initial validation and submit them to the iOS or Android topics 
Based on the event types and the source an event arrives from,Event Proxy determines what Kafka topic the event
will be submitted.


com.ibm.airlytics.consumer.purchase.AndroidPurchaseConsumer is a Kafka consumer that listens 
to two Kafka events topics [Android<Product>Purchase, Android<Product>Notification] 
Each iOS event contains only receipt meta-data and the current subscription state

The main AndroidPurchaseConsumer purpose is to recognise the notification type,retrieve,parse and insert in
purchases related DB table, later this data will be queried by Analytics tools for build various cohorts.

See com.ibm.airlytics.consumer.purchase.PurchaseConsumerConfig the Purchase consumer configuration base classes.
The configuration holds common Consumer parameters plus extra set specific for the Purchase consumer.

Environment variables configuration example:
```yaml
      containers:
        - name: purchase-consumer
          image: myimagestore.com/airlytics/consumer:latest
          args: ["android_purchase"]
          ports:
            - containerPort: 8084
              name: metrics
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'Android Product'
            - name: METRICS_PORT
              value:  '8084'
            - name: AIRLYTICS_CONSUMER_NAME
              value:  'android purchase'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
            - name: USERDB_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: userdb-secret
                      key: airlytics-all-rw

```

Airlock configuration example:
```json
{
    "maxPollIntervalMs": 300000,
    "securityProtocol": "SSL",
    "bootstrapServers": "kafka1.mykafka.com:9094,kafka2.mykafka.com:9094",
    "maxPollRecords": 600, 
    "consumerGroupId":"PURCHASE_CONSUMER_ANDROIDPRODUCT",
    "notificationsTopic":"AndroidProductNotifications",
    "eventProxyApiKey":"<eventProxyApiKey>",
    "eventProxyDevApiKey":"<eventProxyDevApiKey>",
    "sqlStatesContinuationPrefixes":[],
    "airlockProductId":"<airlockProductId>",
    "errorsTopic":"AndroidProductPurchaseConsumerErrors",
    "topics":["AndroidProductPurchases",
    "AndroidProductNotifications",
    ],
    "updatePurchaseRenewalStatusInterval":86400,
    "updatePurchaseRenewalStatusInitDelay":3600,
    "updatePurchaseRenewalStatusShortInterval":3600,
    "fullPurchaseUpdateInterval":2592000,
    "fullPurchaseUpdateInitDelay":2592000,
    "purchasesEventsTable":"android_[product].purchase_events",
    "purchasesEventsDevTable":"android_[product]_dev.purchase_events",
    "purchasesTable":"android_[product].purchases",
    "purchasesUsersTable":"android_[product].purchases_users",
    "purchasesDevTable":"android_[product]_dev.purchases",
    "purchasesUsersDevTable":"android_[product]_dev.purchases_users",
    "fixingBuggyRows":true
}
```
