# Proxy Error Resend Job

## Introduction

A service that takes rawevents from proxy errors topic's dump, fixes them, and resends to proxy.

## Key Classes

**com.ibm.weather.airlytics.jobs.eventspatch.services.JobExecutionService** executes the pre-configured patch (see below) 
upon application startup and closes the application upon completion.

All patch classes must extend the abstract base class **com.ibm.weather.airlytics.jobs.eventspatch.patcher.AbstractPatch** with 
a patch specific **com.ibm.weather.airlytics.common.athena.QueryBuilder** implementation for Athena query selecting thew patched events
and a patch-cpecific **com.ibm.weather.airlytics.jobs.eventspatch.patcher.AirlyticsEventPatcher** implementation that changes 
the data in the selected events before re-sending them to the configured Airlytics Events Proxy API. 

## Service Configuration

The service requires setting a number of environment variables. Here is an example of environment configuration for a pod in Kubernetes:
```yaml
      apiVersion: batch/v1
      kind: Job
      metadata:
        name: proxy-error-resend-helloworld-prod
        namespace: airlytics
        labels:
          app: proxy-error-resend
        annotations:
          deployment.kubernetes.io/revision: '3'
      spec:
        ttlSecondsAfterFinished: 172800
        template:
          metadata:
            labels:
              app: proxy-error-resend
          spec:
            nodeSelector:
              pod-type: memory64G
            containers:
              - name: proxy-error-resend
                image: my.imagestore.com/airlytics/proxy-error-resend-job:latest
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
                livenessProbe:
                  httpGet:
                    path: /healthcheck/is-alive
                    port: metrics
                  initialDelaySeconds: 90
                  periodSeconds: 30
                readinessProbe:
                  httpGet:
                    path: /healthcheck/is-ready
                    port: metrics
                  initialDelaySeconds: 60
                  periodSeconds: 30
                resources:
                  limits:
                    memory: 4Gi
                  requests:
                    memory: 4Gi
                terminationMessagePath: /dev/termination-log
                terminationMessagePolicy: File
                imagePullPolicy: Always
            terminationGracePeriodSeconds: 30
            dnsPolicy: ClusterFirst
            securityContext: {}
            schedulerName: default-scheduler
            restartPolicy: Never
        backoffLimit: 0
```

Airlock configuration for the service may look like
```json
{
  "patchName": "RegisteredUserIdPatch20211226",
  "patchStartPartition": 0,
  "athenaRegion": "us-east-1",
  "athenaResultsOutputBucket": "aws-athena-query-results",
  "athenaCatalog": "AwsDataCatalog",
  "eventProxyIntegrationConfig": {
    "eventApiEnabled": true,
    "eventApiBatchSize": 100,
    "eventApiRateLimit": 100
  },
  "athenaDb": "hello_world",
  "athenaTable": "prod_proxy_error_rawevents",
  "eventApiClientConfig": {
    "eventApiBaseUrl": "https://airlytics.airlock.myserver.com",
    "eventApiPath": "/eventproxy/track",
    "eventApiKey": "54321"
  }
}
```