# Airlytics Amplitude History Upload Job

## Overview

A service to export historical users data from Airlytics Users DB to Amplitude:
* Users with essential attributes
* Purchase events
* Dummy events to enable all history users in Amplitude

## Key Classes

**com.ibm.weather.airlytics.amplitude.service.AmplitudeHistoryExtractionService** starts the history upload upon 
application startup.

## Service Configuration

The service requires setting a number of environment variables. Here is an example of environment configuration for a pod in Kubernetes:
```yaml

containers:
  - name: amplitude-history-upload
    image: my.imagestore.com/airlytics/amplitude-history-upload:latest
    ports:
      - containerPort: 8081
        name: web
        protocol: TCP
    env:
      - name: USERS_PLATFORM
        value: 'ios'
      - name: AMPLITUDE_API_ENABLED
        value: 'true'
      - name: USERS_SHARD
        value: '0'
      - name: SUBS_SHARD
        value: '0'
      - name: EVENTS_SHARD
        value: '0'
      - name: SHARDS_PAUSE_AFTER
        value: '20'
      - name: SHARDS_PAUSE_MS
        value: '100000'
      - name: AMPLITUDE_API_KEY
        valueFrom:
          secretKeyRef:
            name: third-party-api-secret
            key: amplitude-ios-key
      - name: DB_URL
        value: 'jdbc:postgresql://airlytics-userdb-services-ro-prod.mydbserver.com:5432/users?currentSchema=users'
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
```

