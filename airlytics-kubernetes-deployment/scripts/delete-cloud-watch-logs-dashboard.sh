#!/bin/bash

#load cluser config
source ./cluster.properties

# login into cluster
chmod +x login_cluster.sh
./login_cluster.sh
source ./cloud-watch-logs-dashboard.sh

for i in "${services[@]}"
do
   echo "delete $i logs dashboard"
   aws cloudwatch delete-dashboards --dashboard-name $i --profile $PROFILE
done


for i in "${consumers[@]}"
do
   echo "delete $i logs dashboard"
   aws cloudwatch delete-dashboards --dashboard-name $i --profile $PROFILE
done
