
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

public class RideShareStreamerV1 extends RideShareStreamerBase {
    public static void main(String[] args) {

        RideShareStreamerV1 rideShareStreamer = new RideShareStreamerV1();
        rideShareStreamer.processStream();
    }

    @Override
    GenericRecord processMessage(GenericRecord val) {
        return val;
    }

}
