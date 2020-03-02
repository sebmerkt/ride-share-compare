#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"
LOG_DIR="/home/"$USER"/log_pipeline/"
mkdir -p $LOG_DIR

# Store PIDs
PID_FILE="$LOG_DIR/pid.dat"
> $PID_FILE

# Start producers

cd $SCRIPT_DIR"/../Kafka/RideShareProducer/target/"

nohup java -jar RideShareProducerV4.jar "/home/$USER/nyc-taxi-rideshare/trip_data/yellow_tripdata_2015-06.csv" 2>&1 > "$LOG_DIR/RideShareProducerV4.log" & echo "$!" >> $PID_FILE
echo "Producer 1 running with PID $!"

nohup java -jar RideShareProducerV4.jar "/home/$USER/nyc-taxi-rideshare/trip_data/green_tripdata_2015-06.csv" 2>&1 > "$LOG_DIR/RideShareProducerV4.log" & echo "$!" >> $PID_FILE
echo "Producer 2 running with PID $!"

nohup java -jar BikeShareProducerV1.jar "/home/$USER/nyc-taxi-rideshare/schema_evolution_data/201909-citibike-tripdata.csv" > "$LOG_DIR/BikeShareProducerV1.log" 2>&1 & echo "$!" >> $PID_FILE
echo "Producer 3 running with PID $!"

# Start stream processing

cd $SCRIPT_DIR"/../Kafka/RideShareStreamer/target/"

nohup java -jar "RideShareStreamerV4.jar" > "$LOG_DIR/RideShareStreamerV4.log" 2>&1 & echo "$!" >> $PID_FILE
echo "Streamer 1 running with PID $!"

nohup java -jar "BikeShareStreamerV1.jar" > "$LOG_DIR/BikeShareStreamerV1.log" 2>&1 & echo "$!" >> $PID_FILE
echo "Streamer 2 running with PID $!"

# Start consumers

cd $SCRIPT_DIR"/../Kafka/RideShareConsumer/target/"

nohup java -jar "RideShareConsumerV4.jar" > "$LOG_DIR/RideShareConsumerV4.log" 2>&1 & echo "$!" >> $PID_FILE
echo "Consumer 1 running with PID $!"

nohup java -jar "BikeShareConsumerV1.jar" > "$LOG_DIR/BikeShareConsumerV1.log" 2>&1 & echo "$!" >> $PID_FILE
echo "Consumer 2 running with PID $!"


