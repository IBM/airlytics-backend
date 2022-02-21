#!/bin/bash

#load cluser config
source ./cluster.properties

echo "Login to:"
echo "Region:"$REGION
echo "Cluster name:"$CLUSTER_NAME

aws eks --region $REGION  update-kubeconfig --name $CLUSTER_NAME --profile  $PROFILE
kubectl config set-context --current --namespace=airlytics