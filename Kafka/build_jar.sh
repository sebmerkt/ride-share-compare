#!/bin/bash


SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"

# pull code
git pull

# Build producers

cd $SCRIPT_DIR"/RideShareProducer/"

mvn clean compile package

cd $SCRIPT_DIR"/RideShareStreamer/"

mvn clean compile package

cd $SCRIPT_DIR"/RideShareConsumer/"

mvn clean compile package