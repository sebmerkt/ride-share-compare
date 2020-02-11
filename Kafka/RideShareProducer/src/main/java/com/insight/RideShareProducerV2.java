
//////////////////////////////////////////////////////////////////////////
//                                                                      //
//  Source file for class RideShareProducerV2                           //
//                                                                      //
//  Description: Producer V2 ingests data corresponding to schema       //
//               version 2                                              //
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

// Implementation of RideShareProducerV2 that produces messages of schema type 2
public class RideShareProducerV2 extends RideShareProducerBase <Ride2> {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        // Initialize Kafka properties
        final Properties props = initProperties();

        // Initialize class instance
        RideShareProducerV2 rideShareProducer = new RideShareProducerV2( );

        // Initialize schema class instance
        rideShareProducer.setRide( new Ride2() );

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
        ride.setFareAmt( InsertDouble(message[9]) );
        ride.setTipAmt( InsertDouble(message[10]) );
        ride.setTotalAmt( InsertDouble(message[11]) );
        ride.setProcessTime( InsertString(ts.toString()) );
    }

}

