#!/bin/bash


SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"

# pull code
git pull

# Build producers

cd $SCRIPT_DIR"/Kafka/RideShareProducer/"

mvn clean compile package

cd $SCRIPT_DIR"/Kafka/RideShareStreamer/"

mvn clean compile package

cd $SCRIPT_DIR"/Kafka/RideShareConsumer/"

mvn clean compile package