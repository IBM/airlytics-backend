#!/bin/bash

if [ "$1" != "" ]; then
    echo "Product name: " $1
else
    echo "Product name parameter is missing"
    exit
fi

if [ "$2" != "" ]; then
    echo "zookeeper: " $2
else
    echo "zookeeper parameter is missing"
    exit
fi

/home/ec2-user/create_topic.sh $1 2 uncompressed 1209600000 $2
/home/ec2-user/create_topic.sh $1ProxyErrors 2 uncompressed 1209600000 $2
/home/ec2-user/create_topic.sh $1DbConsumerErrors 2 uncompressed 1209600000 $2
/home/ec2-user/create_topic.sh $1PersistenceConsumerErrors 2 uncompressed 1209600000 $2
/home/ec2-user/create_topic.sh $1Dev 2 uncompressed 1209600000 $2
/home/ec2-user/create_topic.sh $1ProxyErrorsDev 2 uncompressed 1209600000 $2
/home/ec2-user/create_topic.sh $1DbConsumerErrorsDev 2 uncompressed 1209600000 $2
/home/ec2-user/create_topic.sh $1PersistenceConsumerErrorsDev 2 uncompressed 1209600000 $2
/home/ec2-user/create_topic.sh $1DSRRequests 2 uncompressed 1209600000 $2
/home/ec2-user/create_topic.sh $1DSRErrors 2 uncompressed 1209600000 $2
/home/ec2-user/create_topic.sh $1Notifications 2 uncompressed 1209600000 $2
/home/ec2-user/create_topic.sh $Purchases 2 uncompressed 1209600000 $2
/home/ec2-user/create_topic.sh $PurchaseConsumerErrors 2 uncompressed 1209600000 $2
/home/ec2-user/create_topic.sh LTVInput 2 uncompressed 1209600000 $2
/home/ec2-user/create_topic.sh $PurchaseConsumerErrors 2 uncompressed 1209600000 $2