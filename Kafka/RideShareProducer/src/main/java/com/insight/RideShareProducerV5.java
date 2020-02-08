package com.insight;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.util.Properties;

public class RideShareProducerV5 extends RideShareProducerBase <Ride5> {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        final Properties props = initProperties();

        RideShareProducerV5 rideShareProducer = new RideShareProducerV5( );

        rideShareProducer.setRide( new Ride5() );

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
        ride.setPassengerCount( InsertInt(message[3]) );
        ride.setTripDistance( InsertDouble(message[4]) );
        ride.setStartLon( 0.0 );
        ride.setStartLat( 0.0 );
        ride.setPULocationID( InsertLong(message[5]) );
        ride.setRateCode( InsertDouble(message[6]) );
        ride.setStoreAndForward( InsertDouble(message[7]) );
        ride.setEndLon( 0.0 );
        ride.setEndLat( 0.0 );
        ride.setDOLocationID( InsertLong(message[8]) );
        ride.setPaymentType( InsertString(message[9]) );
        ride.setFareAmt( InsertDouble(message[10]) );
        ride.setExtra( InsertDouble(message[11]) );
        ride.setSurcharge( InsertDouble(message[12]) );
        ride.setMtaTax( InsertDouble(message[13]) );
        ride.setTipAmt( InsertDouble(message[14]) );
        ride.setTollsAmt( InsertDouble(message[15]) );
        ride.setTotalAmt( InsertDouble(message[16]) );
    }
}
