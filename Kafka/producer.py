#!/bin/python

from kafka import KafkaProducer
import pandas as pd
import boto3
import time
import random

bucket_name = "nyc-taxi-rideshare"
# file_name = "trip_data/fhv_tripdata_2015-01.csv"


# Producer running on one (and only one) of the Kafka nodes
producer = KafkaProducer(bootstrap_servers = 'localhost:9092')


s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)
# Iterates through all the objects, doing the pagination for you. Each obj
# is an ObjectSummary, so it doesn't contain the body. You'll need to call
# get to get the whole body.
try:
  for obj in bucket.objects.all():
    if  obj.size > 0:
      key = obj.key
      if 'green_tripdata_2013-08' in key:
        body = obj.get()['Body']

        data = pd.read_csv(body)
        #read through each line of csv and send the line to the kafka topic
        for index, row in data.iterrows():
            output = ''
            for element in row:
                output = output + str(element) + ","
            
            producer.send('taxiinput', output.encode())
            producer.flush()
            slp=0.01*random.randint(1, 100)
            time.sleep(slp)

except KeyboardInterrupt:
    print('Interrupted')
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)




