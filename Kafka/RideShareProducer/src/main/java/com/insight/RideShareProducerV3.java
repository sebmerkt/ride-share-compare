
//////////////////////////////////////////////////////////////////////////
//                                                                      //
//  Source file for class RideShareProducerV3                           //
//                                                                      //
//  Description: Producer V3 ingests data corresponding to schema       //
//               version 3                                              //
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

// Implementation of RideShareProducerV3 that produces messages of schema type 3
public class RideShareProducerV3 extends RideShareProducerBase <Ride3> {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        // Initialize Kafka properties
        final Properties props = initProperties();

        // Initialize class instance
        RideShareProducerV3 rideShareProducer = new RideShareProducerV3( );

        // Initialize schema class instance
        rideShareProducer.setRide( new Ride3() );

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

