
//////////////////////////////////////////////////////////////////////////
//                                                                      //
//  Source file for class RideShareStreamerV1                           //
//                                                                      //
//  Description: Streamer V1 processes messages corresponding to schema //
//               version 1  (V1 does not require processing)            //
//                                                                      //
//  Author: Sebastian Merkt (@sebmerkt)                                 //
//                                                                      //
//////////////////////////////////////////////////////////////////////////

package com.insight;

import org.apache.avro.generic.GenericRecord;

// Implementation of RideShareStreamerV1 that consumes messages of schema type 1
public class RideShareStreamerV1 extends RideShareStreamerBase {
    public static void main(String[] args) {

        // Initialize class instance and process stream
        RideShareStreamerV1 rideShareStreamer = new RideShareStreamerV1();
        rideShareStreamer.processStream();
    }

    @Override
    GenericRecord processMessage(GenericRecord val) {
        return val;
    }

}
