# ride-share-compare
Insight Data Engineering project 2020A


Ride-Share-Compare is presented [here](https://docs.google.com/presentation/d/1tzfh4vnOFDyHu2FrjZmUu_YrsaVfcyU1XYJdBHj6pCk/edit#slide=id.p).

<hr/>

## How to install and get it up and running

Install the Confluent Platform using the Ansible Playbook from

# Kafka

Build all versions of the Kafka producers, consumers and stream processing java applications. Run the

```bash build_jar.sh```

Add two new Kafka topic

```/usr/bin/kafka-topics --create --zookeeper <broker-address> --topic XXXX --partitions <num-partitions> --replication-factor <num-replications>```

where XXXX is replaced with the two topic names XXXX and XXXX.



Run the producers

```java -jar RideShareProducer/target/RideShareProducerV<version>.jar /path/to/input-file.csv```

where the input file is a csv file from the NYC taxi data or Citi bike data.

Run the stream processing application

```java -jar RideShareStreamer/target/RideShareStreamerV<version>.jar```

Run the consumer

```java -jar RideShareConsumer/target/RideShareConsumerV<version>.jar```


# Database

Create the required table

```XXXX```

# Dash

Run the Dash application

```python
python app.py
```

<hr/>

## Introduction

Ride-share providers like Lyft and Uber are getting more and more popular. As their popularity increases, so does the number of available ride-share providers. Ride-share users 


## Architecture

The data resides in a Amazon S3 bucket from where it is streamed into Apache Kafka. A Confluent Kafka cluster is set up on 4 Amazon EC2 nodes. 

A PostGIS server resides on an additional EC2 node

The Plotly Dash web application is hosted on another EC2 node.


## Dataset

Real-time ride-share data is not easily available. Therefore, the New York City taxi dataset is used. The dataset is available at
XXX
Using a pre-existing dataset instead of real-time data also allows for the simulation of an evolving schema. 

In addition, Citi Bike data is used. The data is available at
XXX


## Engineering challenges

The ride-share data evolves over time. New versions of the Kafka applications account for the change in the data's schema. In order to not cause downtime of the pipeline, the update to newer code versions needs to happen during production time. Confluent Kafka uses a schema registry that allows the evolution of the data schema. Adding default values to the data schema allows for full backward and forward compatibility between old and new versions of the Kafka applications. These versions can then run in simultaneously


## Trade-offs

Trade-off had to be made in storing the incoming data. 


## Rolling deployment

The Kafka applications can be started and stopped when desired. New versions of the applications can be started at any time. A script that subsequently runs new versions of the producers, consumers and stream processors, thereby evolving the schema can by run

```bash run_rolling_deployment.sh```

The script also evolves the database to accommodate the evolving data.

