
//////////////////////////////////////////////////////////////////////////
//                                                                      //
//  Source file for class RideShareProducerV4                           //
//                                                                      //
//  Description: Producer V4 ingests data corresponding to schema       //
//               version 4                                              //
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

// Implementation of BikeShareProducerV1 that produces messages of schema type 4
public class BikeShareProducerV1 extends RideShareProducerBase <Bike1> {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        // Initialize Kafka properties
        final Properties props = initProperties();

        // Initialize class instance
        BikeShareProducerV1 rideShareProducer = new BikeShareProducerV1( );

        // Initialize schema class instance
        rideShareProducer.setRide( new Bike1() );

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
        ride.setVendorName( "Citi Bike" );
        ride.setTripduration( InsertLong(message[0]) );
        ride.setTripPickupDateTime( InsertString(message[1]) );
        ride.setTripDropoffDateTime( InsertString(message[2]) );
        ride.setStartStationId( InsertLong(message[3]) );
        ride.setStartStationName( InsertString(message[4]) );
        ride.setStartLat( InsertDouble(message[5]) );
        ride.setStartLon( InsertDouble(message[6]) );
        ride.setEndStationId( InsertLong(message[7]) );
        ride.setEndStationName( InsertString(message[8]) );
        ride.setEndLat( InsertDouble(message[9]) );
        ride.setEndLon( InsertDouble(message[10]) );
        ride.setBikeid( InsertLong(message[11]) );
        ride.setUsertype( InsertString(message[12]) );
        ride.setBirthYear( InsertLong(message[13]) );
        ride.setGender( InsertLong(message[14]) );
        ride.setProcessTime( InsertString(ts.toString()) );
    }
}
