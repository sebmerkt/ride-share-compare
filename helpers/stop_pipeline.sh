#!/bin/bash

# Get PIDs of the Kafka apps
LOG_DIR="/home/"$USER"/log_pipeline/"
PID_FILE="$LOG_DIR/pid.dat"

while read p; do
  kill -s 9 "$p"
done <$PID_FILE