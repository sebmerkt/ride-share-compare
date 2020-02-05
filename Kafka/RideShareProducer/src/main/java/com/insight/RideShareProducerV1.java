package com.insight;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.util.Properties;

public class RideShareProducerV1 extends RideShareProducerBase <Ride1> {

    @SuppressWarnings("InfiniteLoopStatement")
    public void main(final String[] args) throws IOException {

        final Properties props = initProperties();

        // construct kafka producer.
        final KafkaProducer<String, Ride1> producer = new KafkaProducer<>(props);

        Ride1 ride = new Ride1();

        sendRecords(args, ride, producer);
        producer.flush();
        producer.close();
    }

    @Override
    void buildRecord(final Ride1 ride, final String[] message) {
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

