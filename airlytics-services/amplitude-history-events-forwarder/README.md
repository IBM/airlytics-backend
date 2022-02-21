# Airlytics Aplitude History Events Forwarder Job

## Overview

A service to export historical Airlytics events from Airlytics Athena DB to Amplitude.

## Key Classes

**com.ibm.weather.airlytics.amplitude.service.AmplitudeHistoryExtractionService** automatically starts the job shortly after
the apllication starts, and exit, when finished.

## Service Configuration

The service requires setting a number of environment variables. Here is an example of environment configuration for a pod in Kubernetes:
```yaml

          env:
            - name: AIRLYTICS_ENVIRONMENT
              value: 'DEV'
            - name: AIRLYTICS_PRODUCT
              value: 'Hello World App'
            - name: AIRLYTICS_DEPLOYMENT
              valueFrom:
                configMapKeyRef:
                  name: airlytics-deployments-config
                  key: airlytics.deployment
            - name: HISTORY_START_DAY
              value: '2021-07-01'
            - name: HISTORY_END_TIME
              value: '1626602400000'
            - name: HISTORY_EVENTS
              value: 'app-launch,asset-viewed,location-viewed'
            - name: HISTORY_MAX_APPVERSION
              value: '10.34.0'
            - name: ATHENA_REGION
              value: 'us-east-1'
            - name: ATHENA_OUTPUT
              value: 'aws-athena-query-results'
            - name: ATHENA_CATALOG
              value: 'AwsDataCatalog'
            - name: ATHENA_DB
              value: 'hello_world'
            - name: ATHENA_RAWEVENTS
              value: 'dev_rawevents'
            - name: AMPLITUDE_API_ENABLED
              value: 'true'
            - name: AMPLITUDE_API_KEY
              valueFrom:
                secretKeyRef:
                  name: third-party-api-secret
                  key: amplitude-android-dev-key
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
