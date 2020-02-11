
//////////////////////////////////////////////////////////////////////////
//                                                                      //
//  Source file for class RideShareProducerV5                           //
//                                                                      //
//  Description: Producer V5 ingests data corresponding to schema       //
//               version 5                                              //
//                                                                      //
//  Author: Sebastian Merkt (@sebmerkt)                                 //
//                                                                      //
//////////////////////////////////////////////////////////////////////////

package com.insight;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;

// Implementation of RideShareProducerV5 that produces messages of schema type 5
public class RideShareProducerV5 extends RideShareProducerBase <Ride5> {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        // Initialize Kafka properties
        final Properties props = initProperties();

        // Initialize class instance
        RideShareProducerV5 rideShareProducer = new RideShareProducerV5( );

        // Initialize schema class instance
        rideShareProducer.setRide( new Ride5() );

        // Initialize Kafka producer
        rideShareProducer.setProducer( new KafkaProducer<>(props));

        // Send Kafka messages
        rideShareProducer.sendRecords(args);

        // Flush and close the producer
        rideShareProducer.getProducer().flush();
        rideShareProducer.getProducer().close();
    }


    @Override
    void buildRecord( final String[] message) {

        // Create current timestamp to send with the message values
        Date date= new Date();
        Timestamp ts = new Timestamp( date.getTime() );

        // Assign all schema fields with the input values
        ride.setVendorName( InsertString(message[0]) );
        ride.setTripPickupDateTime( InsertString(message[1]) );
        ride.setTripDropoffDateTime( InsertString(message[2]) );
        ride.setPassengerCount( InsertInt(message[3]) );
        ride.setTripDistance( InsertDouble(message[4]) );
        ride.setStartLon( 0.0 );
        ride.setStartLat( 0.0 );
        ride.setRateCode( InsertDouble(message[5]) );
        ride.setStoreAndForward( InsertDouble(message[6]) );
        ride.setPULocationID( InsertLong(message[7]) );
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
        ride.setProcessTime( InsertString(ts.toString()) );
    }
}
