# Airlytics Braze History Upload Job

## Overview

A service to export historical users data from Airlytics Users DB to Braze:
* Install events
* Trial subscription events
* Purchase events
* Autorenewal events
* Subscription expiration events

## Key Classes

**com.ibm.weather.airlytics.braze.service.BrazeHistoryExtractionService** starts the history upload upon
application startup.

## Service Configuration

The service requires setting a number of environment variables. Here is an example of environment configuration for a pod in Kubernetes:
```yaml
containers:
  - name: braze-history-upload
    image: my.imagestore.com/airlytics/braze-history-upload:latest
    ports:
      - containerPort: 8081
        name: web
        protocol: TCP
    env:
      - name: USERS_PLATFORM
        value: 'ios'
      - name: BRAZE_API_ENABLED
        value: 'true'
      - name: INSTALL_TRIAL_SHARD
        value: '85'
      - name: SUB_SHARD
        value: '0'
      - name: SHARDS_PAUSE_AFTER
        value: '20'
      - name: SHARDS_PAUSE_MS
        value: '100000'
      - name: BRAZE_API_KEY
        valueFrom:
          secretKeyRef:
            name: third-party-api-secret
            key: braze-ios-key
      - name: DB_URL
        value: 'jdbc:postgresql://airlytics-userdb-services-ro-prod.mydbservice.com:5432/users?currentSchema=users'
      - name: DB_USER_NAME
        value: 'airlytics_all_ro'
      - name: DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: userdb-secret
            key: airlytics-all-ro
    livenessProbe:
      httpGet:
        path: /healthcheck/is-alive
        port: web
      initialDelaySeconds: 30
      periodSeconds: 30
    readinessProbe:
      httpGet:
        path: /healthcheck/is-ready
        port: web
      initialDelaySeconds: 30
      periodSeconds: 30
```

