
//////////////////////////////////////////////////////////////////////////
//                                                                      //
//  Source file for class RideShareStreamerV2                           //
//                                                                      //
//  Description: Streamer V2 processes messages corresponding to schema //
//               version 2                                              //
//                                                                      //
//  Author: Sebastian Merkt (@sebmerkt)                                 //
//                                                                      //
//////////////////////////////////////////////////////////////////////////

package com.insight;

import org.apache.avro.generic.GenericRecord;

// Implementation of RideShareStreamerV2 that consumes messages of schema type 2
public class RideShareStreamerV2 extends RideShareStreamerBase {

    public static void main(String[] args) {

        // Initialize class instance and process stream
        RideShareStreamerV2 rideShareStreamer = new RideShareStreamerV2();
        rideShareStreamer.processStream();
    }

    static GenericRecord processMessage(GenericRecord val) {
        // Newer schema have integer codes: 1= Creative Mobile Technologies (CMT), LLC; 2= VeriFone Inc. (VTS)
        if ( val.get("vendor_name") == "1" ) {
            val.put("vendor_name", String.valueOf("CMT"));
        }
        else if ( val.get("vendor_name") == "2" ) {
            val.put("vendor_name", String.valueOf("VTS"));
        }


        return val;
    }


}



