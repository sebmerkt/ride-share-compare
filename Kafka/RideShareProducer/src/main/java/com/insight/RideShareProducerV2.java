package com.insight;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class RideShareProducerV2 extends RideShareProducerBase <Ride2> {

    @SuppressWarnings("InfiniteLoopStatement")
    public void main(final String[] args) throws IOException {

        final Properties props = initProperties();

        // construct kafka producer.
        final KafkaProducer<String, Ride2> producer = new KafkaProducer<>(props);

        Ride2 ride = new Ride2();

        sendRecords(args, ride, producer);
        producer.flush();
        producer.close();
    }


    @Override
    void buildRecord(final Ride2 ride, final String[] message) {
        ride.setVendorName( InsertString(message[0]) );
        ride.setTripPickupDateTime( InsertString(message[1]) );
        ride.setTripDropoffDateTime( InsertString(message[2]) );
        ride.setTripDistance( InsertDouble(message[3]) );
        ride.setStartLon( InsertDouble(message[4]) );
        ride.setStartLat( InsertDouble(message[5]) );
        ride.setEndLon( InsertDouble(message[6]) );
        ride.setEndLat( InsertDouble(message[7]) );
        ride.setTotalAmt( InsertDouble(message[8]) );
    }

}

