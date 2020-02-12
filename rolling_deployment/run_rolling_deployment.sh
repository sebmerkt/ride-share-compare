#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"

# Start code version 1
# Start streamer

cd $SCRIPT_DIR"/../Kafka/RideShareStreamer/target/"

nohup java -jar RideShareStreamerV1.jar
& stream_v1_process_id=$!

# Start consumer

cd $SCRIPT_DIR"/../Kafka/RideShareConsumer/target/"

nohup java -jar RideShareConsumer1.jar
& cons_v1_process_id=$!

# Start producer

cd $SCRIPT_DIR"/../Kafka/RideShareProducer/target/"

nohup java -jar RideShareProducerV1.jar "$($SCRIPT_DIR"/../nyc-taxi-rideshare/schema_evolution_data/yellow_tripdata_2009-01.csv")"
& prod_v1_process_id=$!




# Let it run for a while

sleep 3m




# Add new schema version 2
# Eveolve database to accomodate schema version 2: Ride2
python update_database.py "Passenger_Count" "int8" "Fare_Amt" "float8" "Tip_Amt" "float8"
& db_process_id=$!

wait $db_process_id

echo Database altere with status $?

# Start new producer/streamer/consumer

cd $SCRIPT_DIR"/../Kafka/RideShareStreamer/target/"

nohup java -jar RideShareStreamerV2.jar
& stream_v2_process_id=$!

cd $SCRIPT_DIR"/../Kafka/RideShareConsumer/target/"

nohup java -jar RideShareConsumer2.jar
& cons_v2_process_id=$!

cd $SCRIPT_DIR"/../Kafka/RideShareProducer/target/"

nohup java -jar RideShareProducerV2.jar "$($SCRIPT_DIR"/../nyc-taxi-rideshare/schema_evolution_data/yellow_tripdata_2009-02.csv")"
& prod_v2_process_id=$!




# Wait until schema 1 is "obsolete" and terminate code version 1

sleep 3m

kill -s 9 $prod_v1_process_id
kill -s 9 $stream_v1_process_id
kill -s 9 $scons_v1_process_id

