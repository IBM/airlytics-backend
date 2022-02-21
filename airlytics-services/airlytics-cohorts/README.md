# Airlytics Cohorts Service

A service to calculate user audiences (cohorts), scheduled or on demand and export them to third party platforms 
used for data analysis and data driven actions.

## Requirements

Airlytics Cohorts Service is used by Airlock Cohorts feature running on Airlock platform using its Airlock BE API,
or Airlock GUI panel. To use the API, or the panel, one needs credentials for Airlock.

## Key Classes

The service provides RESTful API for CRUD operations on user cohorts. See
**com.ibm.weather.airlytics.cohorts.api.CohortCalculationController** Swagger-annotated Spring controller.

The service relies on Airlock BE Cohorts API to obtain cohort definitions, and report back cohort calculation and export 
status. See **com.ibm.weather.airlytics.cohorts.integrations.AirlockCohortsClient** class implementing all these API calls.

## Service Configuration

The service requires setting a number of environment variables. Here is an example of environment configuration for a pod in Kubernetes:
```yaml
      containers:
        - name: air-cohorts
          image: my.imagestore.com/airlytics/air-cohorts:latest
          ports:
            - containerPort: 8081
              name: web
              protocol: TCP
          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'PROD'
            - name: AIRLYTICS_PRODUCT
              value: 'Hello World App'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
            - name: DB_URL
              value: 'jdbc:postgresql://airlytics-userdb-prod.dbserver.com:5432/users?currentSchema=hello_world'
            - name: DB_USER_NAME
              value: 'airlytics_cohorts'
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: userdb-secret
                  key: airlytics-cohorts
            - name: RO_DB_URL
              value: 'jdbc:postgresql://airlytics-userdb-ro-prod.dbserver.com:5432/users?currentSchema=hello_world'
            - name: RO_DB_USER_NAME
              value: 'airlytics_cohorts'
            - name: RO_DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: userdb-secret
                  key: airlytics-cohorts
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
```

Airlock configuration for the service may look like
```json
{
  "kafkaEnabled": true,
  "kafkaSecurityProtocol": "SSL",
  "productIds": [
    "12345"
  ],
  "kafkaTopic": "AirCohorts",
  "additionalTables": [
    "users_pi",
    "user_features_session",
    "purchases_with_userid"
  ],
  "kafkaBootstrapServers": "kafka-1.myserver.com:9094,kafka-2.myserver.com:9094",
  "airlockApiBaseUrl": "https://adminapi.airlock.myserver.com/airlock/api"
}
```