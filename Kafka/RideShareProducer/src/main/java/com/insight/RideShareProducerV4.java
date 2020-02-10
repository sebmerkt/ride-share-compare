
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

public class RideShareProducerV4 extends RideShareProducerBase <Ride4> {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        final Properties props = initProperties();

        RideShareProducerV4 rideShareProducer = new RideShareProducerV4( );

        rideShareProducer.setRide( new Ride4() );

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
        ride.setRateCode( InsertDouble(message[7]) );
        ride.setStoreAndForward( InsertDouble(message[8]) );
        ride.setEndLon( InsertDouble(message[9]) );
        ride.setEndLat( InsertDouble(message[10]) );
        ride.setPaymentType( InsertString(message[11]) );
        ride.setFareAmt( InsertDouble(message[12]) );
        ride.setSurcharge( InsertDouble(message[13]) );
        ride.setMtaTax( InsertDouble(message[14]) );
        ride.setTipAmt( InsertDouble(message[15]) );
        ride.setTollsAmt( InsertDouble(message[16]) );
        ride.setTotalAmt( InsertDouble(message[17]) );
        ride.setProcessTime( InsertString(ts.toString()) );
    }
}
