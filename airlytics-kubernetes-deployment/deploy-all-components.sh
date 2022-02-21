#!/bin/bash

#load cluster config
source ./cluster.properties

 # login cluster
chmod +x login_cluster.sh
./login_cluster.sh

echo "create namespace"
kubectl apply -f ../manifest

echo "create cluster deployment configmap"
kubectl delete configmap airlytics-deployments-config -n $NAMESPACE
kubectl create configmap airlytics-deployments-config \
--from-literal event.proxy.config.bucket=$EVENT_PROXY_CONFIG_BUCKET \
--from-literal kafak.brokers.connectors=$KAFKA_BROKER_CONNECTORS \
--from-literal airlytics.deployment=$AIRLYTICS_DEPLOYMENT \
--from-literal airlytics.environment=$AIRLYTICS_ENVIRONMENT \
-n $NAMESPACE


echo "deploy prometheus operator"
kubectl apply -f ../prometheus/manifests/setup --validate=false

echo "deploy cloud watch"
kubectl delete configmap cluster-info -n $NAMESPACE
kubectl create configmap cluster-info --from-literal cluster.name=$CLUSTER_NAME --from-literal logs.region=$REGION -n $NAMESPACE
kubectl delete configmap cluster-info -n amazon-cloudwatch
kubectl create configmap cluster-info --from-literal cluster.name=$CLUSTER_NAME --from-literal logs.region=$REGION -n amazon-cloudwatch

kubectl apply -f ../amazon-cloudwatch/cloudwatch-namespace.yaml
kubectl apply -f ../amazon-cloudwatch/cloud-watch-fluentd.yaml

echo "deploy consumers"
kubectl apply -f ../consumers/compaction
kubectl apply -f ../consumers/persistence
kubectl apply -f ../consumers/userdb
kubectl apply -f ../consumers/dsr-consumer
kubectl apply -f ../consumers/raw-data
kubectl apply -f ../consumers/cloning
kubectl apply -f ../consumers/amplitude-transform-consumer
kubectl apply -f ../consumers/amplitude-forward-consumer
kubectl apply -f ../consumers/braze-transform-consumer
kubectl apply -f ../consumers/braze-forward-consumer
kubectl apply -f ../consumers/mparticle-transform-consumer
kubectl apply -f ../consumers/mparticle-forward-consumer
kubectl apply -f ../consumers/amplitude-cohorts
kubectl apply -f ../consumers/mparticle-cohorts
kubectl apply -f ../consumers/ups-cohorts
kubectl apply -f ../consumers/braze-cohorts
kubectl apply -f ../consumers/braze-currents

echo "deploy event proxy"
kubectl apply -f ../event-proxy

echo "deploy air-cohorts"
kubectl apply -f ../services/air-cohorts

echo "deploy ai-data-import"
kubectl apply -f ../services/ai-data-import

echo "deploy kafdrop proxy"
kubectl apply -f ../kafdrop

echo "deploy prometheus"
kubectl apply -f ../prometheus/manifests

echo "deploy retention-tracker"
kubectl apply -f ../retention-tracker

echo "deploy cloudwatch fluentd"
kubectl apply -f ../amazon-cloudwatch

echo "deploy userdb-dumper"
kubectl apply -f ../userdb-dumper

echo "deploy dsr-requests-scrapper"
kubectl apply -f ../dsr-requests-scrapper

echo "create cloudwatch logs"
chmod +x delete-cloud-watch-logs-dashboard.sh
./delete-cloud-watch-logs-dashboard.sh

echo "deploy kubernetes dashboard"
kubectl apply -f ../kubernetes-dashboard

echo "set images version"
./set-images-builds.sh

echo "set grafana password"
GRAFANA_CONTAINER=$(kubectl get pod -n monitoring | grep grafana | awk '{print $1;}')
kubectl exec -ti $GRAFANA_CONTAINER  -n monitoring -- grafana-cli admin reset-admin-password TpmaGdqdGp5anl1
