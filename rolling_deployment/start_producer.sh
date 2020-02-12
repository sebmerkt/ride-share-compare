#!/bin/bash

# Start code version 1

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"

cd $SCRIPT_DIR"/../Kafka/RideShareProducer/target/"

nohup java -jar RideShareProducerV1.jar "$($SCRIPT_DIR"/../nyc-taxi-rideshare/trip_data/yellow_tripdata_2015-01.csv")"


