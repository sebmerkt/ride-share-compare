#!/bin/bash

# Going from schema Ride1 to schema Ride2

python update_database.py "Passenger_Count" "int8" "Fare_Amt" "float8" "Tip_Amt" "float8"
& process_id=$!

wait $process_id

echo Database altere with status $?

