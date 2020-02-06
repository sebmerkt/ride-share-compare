package com.insight;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.util.Properties;

public class RideShareProducerV1 extends RideShareProducerBase <Ride1> {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        final Properties props = initProperties();

        RideShareProducerV1 rideShareProducer = new RideShareProducerV1( );

        rideShareProducer.setRide( new Ride1() );

        rideShareProducer.setProducer( new KafkaProducer<>(props));

        rideShareProducer.sendRecords(args);
        rideShareProducer.getProducer().flush();
        rideShareProducer.getProducer().close();
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

