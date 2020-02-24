# Ride-Share-Compare
### Your ride on your terms
Ride-Share-Compare was my Insight Data Engineering project in the Winter 2020 session. Ride-Share-Compare is presented [here](https://docs.google.com/presentation/d/1tzfh4vnOFDyHu2FrjZmUu_YrsaVfcyU1XYJdBHj6pCk/edit#slide=id.p).

<hr/>

## How to install and get it up and running

Install the Confluent Platform using the Ansible Playbooks. Instructions can be found [here](https://docs.confluent.io/current/installation/installing_cp/cp-ansible.html).

### Environment

Before installation a few environment variables should be set. Edit the file `setup_env.sh` to match your current setup. Then on each node execute

```source setup_env.sh```


### Kafka

Build all versions of the Kafka producers, consumers and stream processing java applications. Run the

```./build_jar.sh```

Add two new Kafka topics

```/usr/bin/kafka-topics --create --zookeeper <broker-address> --topic <topic-name> --partitions <num-partitions> --replication-factor <num-replications>```

where `<topic-name>` is replaced with the two topic names ride-share-input and ride-share-output.



Run the producers

```java -jar RideShareProducer/target/RideShareProducerV<version>.jar /path/to/input-file.csv```

where the arguments are input files from the NYC taxi data or Citi bike data, see in section [Dataset](#dataset).

Run the stream processing application

```java -jar RideShareStreamer/target/RideShareStreamerV<version>.jar```

Run the consumer

```java -jar RideShareConsumer/target/RideShareConsumerV<version>.jar```


### Database

Install PostGIS on one of the nodes. Create the required table in PostGIS by running 

```./rolling_deployment/create_database.py```

### Dash

Run the Dash application

```./app.py```

<hr/>

## Introduction

Ride-share providers like Lyft and Uber are getting more and more popular. As their popularity increases, so does the number of available ride-share providers. Ride-share users want to be able to compare different providers according to their cirrent needs. Whether they need their ride to arrive fast or be cheap, Ride-Share-Compare will give users all the information to make an informated choice.


## Architecture

![alt text](/images/pipeline.png "Pipeline")

The data resides in a Amazon S3 bucket from where it is streamed into Apache Kafka. A Confluent Kafka cluster is set up on 4 Amazon EC2 nodes. The cluster consists of 4 Zookeepers and Kafka Brokers. In additiona, a schema registry is configured that handles the evolution of the data schema. Avro is chosen as a serialization format to work with the schema registry.

A PostGIS server resides on an additional EC2 node. PostGIS was chosen to allow for spatial queries. This allows to filter the data by proximity of a users position.

The Plotly Dash web application is hosted on another EC2 node. Dash was chosen since it easily integrates Mapbox. Custom Mapbox layers display the different New York locations used in later versions of the NYC TLC dataset.


## Dataset

Real-time ride-share data is not easily available. Therefore, the New York City taxi dataset is used. The dataset is published by the [NYC Taxi and Limousine Commission (TLC)](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
Using a pre-existing dataset instead of real-time data also allows for the simulation of an evolving schema. 

In addition, Citi Bike data is used. The data is published by [Citi Bike](https://www.citibikenyc.com/system-data)


## Engineering challenges

The ride-share data evolves over time. New versions of the Kafka applications account for the change in the data's schema. In order to not cause downtime of the pipeline, the update to newer code versions needs to happen during production time. Confluent Kafka uses a schema registry that allows the evolution of the data schema. Adding default values to the data schema allows for full backward and forward compatibility between old and new versions of the Kafka applications. These versions can then run in simultaneously and process and consume messages of all schema versions.


## Trade-offs

Trade-off had to be made in storing the incoming data. One PostGIS database is used for storing the data. This is sufficient for stroring the amount of data coming in in this test version. For an increasing number of ride-share providers as well as when including locations other than just New York City the database will not be able to handle the input. In this case the data should not be put in a SQL database like PostGIS for permanent storage, but rather saved in a NoSQL database. The incoming data should then be processed directly for usage in the web app.


## Rolling deployment

The Kafka java applications can be started and stopped when desired. New versions of the applications can be started at any time. A script that simulates schema evolution by subsequently running new versions of the producers, consumers and stream processors can be found in `rolling_deployment`. Before running the script, a clean database table should be created

```./rolling_deployment/create_database.py```

Afterwards,schema evolution can be simulated by running

```./run_rolling_deployment.sh```

The script also evolves the database to accommodate the evolving data.

