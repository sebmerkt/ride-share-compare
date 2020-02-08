package com.insight;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;

public class RideShareProducerV3 extends RideShareProducerBase <Ride3> {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        final Properties props = initProperties();

        RideShareProducerV3 rideShareProducer = new RideShareProducerV3( );

        rideShareProducer.setRide( new Ride3() );

        rideShareProducer.setProducer( new KafkaProducer<>(props));

        rideShareProducer.sendRecords(args);
        rideShareProducer.getProducer().flush();
        rideShareProducer.getProducer().close();
    }


    @Override
    void buildRecord( final String[] message) {

        Date date= new Date();
        Timestamp ts = new Timestamp( date.getTime() );

        ride.setVendorName( InsertString(message[0]) );
        ride.setTripPickupDateTime( InsertString(message[1]) );
        ride.setTripDropoffDateTime( InsertString(message[2]) );
        ride.setPassengerCount( InsertInt(message[3]) );
        ride.setTripDistance( InsertDouble(message[4]) );
        ride.setStartLon( InsertDouble(message[5]) );
        ride.setStartLat( InsertDouble(message[6]) );
        ride.setEndLon( InsertDouble(message[7]) );
        ride.setEndLat( InsertDouble(message[8]) );
        ride.setPaymentType( InsertString(message[9]) );
        ride.setFareAmt( InsertDouble(message[10]) );
        ride.setTipAmt( InsertDouble(message[11]) );
        ride.setTollsAmt( InsertDouble(message[12]) );
        ride.setTotalAmt( InsertDouble(message[13]) );
        ride.setProcessTime( InsertString(ts.toString()) );
    }
}

