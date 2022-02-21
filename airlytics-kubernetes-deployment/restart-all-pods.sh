#!/bin/bash

#load cluser config
source ./cluster.properties

echo "Restart all pods"
echo "create cluster deployment configmap"
kubectl delete configmap airlytics-deployments-config -n $NAMESPACE
kubectl create configmap airlytics-deployments-config \
--from-literal event.proxy.config.bucket=$EVENT_PROXY_CONFIG_BUCKET \
--from-literal kafak.brokers.connectors=$KAFKA_BROKER_CONNECTORS \
--from-literal airlytics.deployment=$AIRLYTICS_DEPLOYMENT_TYPE \
--from-literal airlytics.environment=$AIRLYTICS_ENVIRONMENT \
-n $NAMESPACE

kubectl delete --all pods -n $NAMESPACE
./set-images-builds.sh

