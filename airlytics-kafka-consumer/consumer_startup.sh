#!/bin/bash
export EC2_AVAIL_ZONE=`curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone --connect-timeout 10`
java -XX:MaxRAMPercentage=90.0 -jar  AirlyticsConsumer-1.0.jar $@