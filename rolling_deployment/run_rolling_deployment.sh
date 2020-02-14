#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"
LOG_DIR="/home/"$USER"/log_rsc/"
mkdir -p $LOG_DIR

# Define input files
INPUT_FILE=( "~/nyc-taxi-rideshare/schema_evolution_data/yellow_tripdata_2009-01_V1_full.csv" "~/nyc-taxi-rideshare/schema_evolution_data/yellow_tripdata_2009-02_V2_full.csv" "~/nyc-taxi-rideshare/schema_evolution_data/yellow_tripdata_2009-03_V3_full.csv" "~/nyc-taxi-rideshare/schema_evolution_data/yellow_tripdata_2009-04_V4_full.csv" "~/nyc-taxi-rideshare/schema_evolution_data/yellow_tripdata_2017-01_V5_full.csv" )

# List to store PIDs
declare -n PIDS="PID$i"

for i in `seq 1 5`;
  do
    if [[ "$i" == "2" ]]; then
      echo "Evolving database"
      python3 $SCRIPT_DIR/update_database.py "Passenger_Count" "int8" "Fare_Amt" "float8" "Tip_Amt" "float8" & db_process_id=$!

      wait $db_process_id
      echo "Database schema evolved to V$i with status "$?
    elif [[ "$i" == "3" ]]; then
      echo "Evolving database"
      python3 $SCRIPT_DIR/update_database.py "Payment_Type" "TEXT" "Tolls_Amt" "float8" & db_process_id=$!

      wait $db_process_id
      echo "Database schema evolved to V$i with status "$?
    elif [[ "$i" == "4" ]]; then
      echo "Evolving database"
      python3 $SCRIPT_DIR/update_database.py "Rate_Code" "float8" "store_and_forward" "float8" "surcharge" "float8" "mta_tax" "float8" & db_process_id=$!

      wait $db_process_id
      echo "Database schema evolved to V$i with status "$?
    fi


    cd $SCRIPT_DIR"/../Kafka/RideShareStreamer/target/"
    
    PRODUCER="RideShareProducerV$i"
    STREAMER="RideShareStreamerV$i"
    CONSUMER="RideShareConsumerV$i"

    echo "${INPUT_FILE[i]}"
    nohup java -jar "$STREAMER.jar" > "$LOG_DIR/$STREAMER.log" 2>&1 & PIDS+=( "$!" )
    echo "Sreamer V$i running with PID $!"

    # Start consumer

    cd $SCRIPT_DIR"/../Kafka/RideShareConsumer/target/"

    nohup java -jar "$CONSUMER.jar" > "$LOG_DIR/$CONSUMER.log" 2>&1 & PIDS+=( "$!" )
    echo "Consumer V$i running with PID $!"

    # Start producer

    cd $SCRIPT_DIR"/../Kafka/RideShareProducer/target/"

    nohup java -jar "$PRODUCER.jar" $INPUT_FILE > "$LOG_DIR/$PRODUCER.log" 2>&1 &  PIDS+=( "$!" )
    echo "Producer V$i running with PID $!"




    # Let it run for a while

    echo "Waiting for 0.5 minutes"
    sleep 30s
  done



for EACH in "${PIDS[@]}"
do
    kill -s 9 "$EACH"
    echo "$EACH terminated with code "$?
done

