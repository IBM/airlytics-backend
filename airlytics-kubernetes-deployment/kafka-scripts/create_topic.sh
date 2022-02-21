#!/bin/bash

if [ "$1" != "" ]; then
    echo "Topic name: " $1
else
    echo "Topic name parameter is missing"
    exit
fi

if [ "$2" != "" ]; then
    echo "Replication factor: " $2
else
    echo "Replication factor parameter is missing"
    exit;
fi

if [ "$3" != "" ]; then
    echo "Compression type: " $3
else
    echo "Compression type parameter is missing"
    exit;
fi
copmression_config_var="compression.type="$3

if [ "$4" != "" ]; then
    echo "Retension time: " $4
else
    echo "Retension time parameter is missing"
    exit;
fi
retension_config_var="retention.ms="$4

if [ "$5" != "" ]; then
    echo "zookeeper: " $5
else
    echo "zookeeper parameter is missing"
    exit;
fi


/home/ec2-user/kafka_2.12-2.2.1/bin/kafka-topics.sh --create --zookeeper $5 \
--replication-factor $2 --partitions 100 --topic $1 --config $copmression_config_var --config $retension_config_var
