#!/bin/bash

if [ "$1" != "" ]; then
    echo "zookeeper: " $1
else
    echo "zookeeper parameter is missing"
    exit
fi

/home/ec2-user/create_topics_for_product.sh <PRODUCT> $1
/home/ec2-user/create_topics_for_product.sh <PRODUCT> $1