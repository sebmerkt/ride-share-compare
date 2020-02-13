#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"
LOG_DIR="/home/"$USER"/log_rsc/"
mkdir -p $LOG_DIR

# Start code version 1
# Start streamer

cd $SCRIPT_DIR"/../Kafka/RideShareStreamer/target/"

nohup java -jar RideShareStreamerV1.jar > $LOG_DIR/RideShareStreamerV1.log 2>&1 & stream_v1_process_id=$!
echo "Sreamer V1 running with PID "$stream_v1_process_id

# Start consumer

cd $SCRIPT_DIR"/../Kafka/RideShareConsumer/target/"

nohup java -jar RideShareConsumerV1.jar > $LOG_DIR/RideShareConsumerV1.log 2>&1 & cons_v1_process_id=$!
echo "Consumer V1 running with PID "$cons_v1_process_id

# Start producer

cd $SCRIPT_DIR"/../Kafka/RideShareProducer/target/"

nohup java -jar RideShareProducerV1.jar ~/nyc-taxi-rideshare/schema_evolution_data/yellow_tripdata_2009-01_V1_full.csv > $LOG_DIR/RideShareProducerV1.log 2>&1 &  prod_v1_process_id=$!
echo "Producer V1 running with PID "$prod_v1_process_id




# Let it run for a while

echo "Waiting for 1 minutes"
sleep 1m




# Add new schema version 2
# Evolve database to accomodate schema version 2: Ride2

echo "Evolving database"
python3 $SCRIPT_DIR/update_database.py "Passenger_Count" "int8" "Fare_Amt" "float8" "Tip_Amt" "float8" & db_process_id=$!

wait $db_process_id
echo "Database schema evolved to V2 with status "$?



# Start new producer/streamer/consumer

cd $SCRIPT_DIR"/../Kafka/RideShareStreamer/target/"

nohup java -jar RideShareStreamerV2.jar > $LOG_DIR/RideShareStreamerV2.log 2>&1 &  stream_v2_process_id=$! 
echo "Streamer V2 running with PID "$stream_v2_process_id

cd $SCRIPT_DIR"/../Kafka/RideShareConsumer/target/"

nohup java -jar RideShareConsumerV2.jar > $LOG_DIR/RideShareConsumerV2.log 2>&1 &  cons_v2_process_id=$!
echo "Consumer V2 running with PID "$cons_v2_process_id

cd $SCRIPT_DIR"/../Kafka/RideShareProducer/target/"

nohup java -jar RideShareProducerV2.jar ~/nyc-taxi-rideshare/schema_evolution_data/yellow_tripdata_2009-02_V2_full.csv > $LOG_DIR/RideShareProducerV2.log 2>&1 & prod_v2_process_id=$!
echo "Producer V2 running with PID "$prod_v2_process_id




# Wait until schema 1 is "obsolete" and terminate code version 1

echo "Waiting for 1 minuts"
sleep 1m

echo "Terminating code V1"
kill -s 9 $prod_v1_process_id
echo "Producer V1 terminated with code "$?
kill -s 9 $stream_v1_process_id
echo "Streamer V1 terminated with code "$?
kill -s 9 $cons_v1_process_id
echo "Consumer V1 terminated with code "$?



# Wait until schema 1 is "obsolete" and terminate code version 1

echo "Waiting for 1 minuts"
sleep 1m

echo "Terminating code V2"
kill -s 9 $prod_v2_process_id
echo "Producer V2 terminated with code "$?
kill -s 9 $stream_v2_process_id
echo "Streamer V2 terminated with code "$?
kill -s 9 $cons_v2_process_id
echo "Consumer V2 terminated with code "$?