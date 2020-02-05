package com.insight;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.util.Properties;

public class RideShareProducerV2 extends RideShareProducerBase <Ride2> {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        RideShareProducerV1 rideShareProducer = new RideShareProducerV1();

        final Properties props = initProperties();

        // construct kafka producer.
        final KafkaProducer<String, Ride2> producer = new KafkaProducer<>(props);

        Ride2 ride = new Ride2();

        rideShareProducer.sendRecords( args );
        producer.flush();
        producer.close();
    }


    @Override
    void buildRecord( final String[] message) {
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

